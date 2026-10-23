using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Logging;
using Prometheus;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

[Collection("integration")]
public sealed class OutboxSlotReaderTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task Read_DecodesInsertsFromSlotWithoutConfirming()
    {
        var schema = await CreateOutboxSchemaAsync();
        var publicationName = IntegrationFixture.NewName();
        await fixture.CreatePublicationAsync(publicationName, schema, new[] { "messages" });
        var slotName = IntegrationFixture.NewName();
        await fixture.CreateLogicalSlotAsync(slotName);
        var confirmedBefore = (await fixture.GetSlotsAsync(slotName)).Single().ConfirmedFlushLsn;

        var (_, logger, channel, cts, reading) = StartReader(schema, publicationName, slotName);
        using var __ = cts;

        await fixture.ExecuteAsync(
            $"INSERT INTO {schema}.messages (topic, key, body, headers, partition, producer_key, created_at) VALUES ('orders.events', '\\x0a0b0c', '\\xdeadbeef', '{{}}', 7, 'cluster-b', '2026-10-17 06:42:11.123456+00'); " +
            $"INSERT INTO {schema}.messages (topic, key, body, headers, created_at) VALUES ('orders.events', '\\x0102', '\\x0304', '{{}}', '2026-10-17 06:42:11.123456+00'); " +
            $"INSERT INTO {schema}.messages (topic, key, body, headers, created_at) VALUES ('billing.events', '\\x05', '\\x06', '{{}}', '2026-10-17 06:42:11.123456+00'); " +
            $"DELETE FROM {schema}.messages");

        var messages = await ReadMessagesAsync(channel, 3, cts.Token);
        messages.Count.ShouldBe(3);

        messages.ShouldAllBe(message => message.Id > 0);
        messages.ShouldAllBe(message => message.Topic == "orders.events" || message.Topic == "billing.events");
        var first = messages.Single(message => message.Topic == "orders.events" && message.Partition == 7);
        first.Key.ShouldBe(new byte[] { 0x0a, 0x0b, 0x0c });
        first.Body.ShouldBe(new byte[] { 0xde, 0xad, 0xbe, 0xef });
        first.Headers.ShouldBe("{}");
        first.ProducerKey.ShouldBe("cluster-b");
        first.CreatedAt.ShouldBe(new DateTime(2026, 10, 17, 6, 42, 11, 123, 456, DateTimeKind.Utc));

        cts.Cancel();
        await SwallowReadingCompletionAsync(reading);

        var confirmedAfter = (await fixture.GetSlotsAsync(slotName)).Single().ConfirmedFlushLsn;
        confirmedAfter.ShouldBe(confirmedBefore);

        logger.Entries.ShouldContain(entry =>
            entry.Level == LogLevel.Information && entry.Message.Contains("декодировано") && entry.Message.Contains("3"));
        logger.Entries.ShouldNotContain(entry => entry.Level == LogLevel.Warning);
    }

    [Fact]
    public async Task Read_IgnoresInsertsOfOtherTablesAndWarns()
    {
        var schema = await CreateOutboxSchemaAsync();
        await fixture.CreateExtraTableAsync(schema, "notes");
        var publicationName = IntegrationFixture.NewName();
        await fixture.CreatePublicationAsync(publicationName, schema, new[] { "messages", "notes" });
        var slotName = IntegrationFixture.NewName();
        await fixture.CreateLogicalSlotAsync(slotName);

        var (_, logger, channel, cts, reading) = StartReader(schema, publicationName, slotName);
        using var __ = cts;

        await fixture.ExecuteAsync($"INSERT INTO {schema}.notes VALUES (1, 'незапрашиваемая таблица')");
        var warned = await WaitForAsync(
            () => logger.Entries.Any(entry =>
                entry.Level == LogLevel.Warning && entry.Message.Contains(schema) && entry.Message.Contains("notes")),
            TimeSpan.FromSeconds(10));
        warned.ShouldBeTrue();
        channel.Reader.TryRead(out _).ShouldBeFalse();

        await fixture.ExecuteAsync($"INSERT INTO {schema}.notes VALUES (2, 'повторная чужая вставка')");
        await fixture.ExecuteAsync(
            $"INSERT INTO {schema}.messages (topic, key, body, headers, created_at) VALUES ('orders.events', '\\x07', '\\x08', '{{}}', '2026-10-17 06:42:11+00')");

        var messages = await ReadMessagesAsync(channel, 1, cts.Token);
        messages.ShouldHaveSingleItem().Key.ShouldBe(new byte[] { 0x07 });

        cts.Cancel();
        await SwallowReadingCompletionAsync(reading);

        logger.Entries.Count(entry => entry.Level == LogLevel.Warning && entry.Message.Contains("notes")).ShouldBe(1);
    }

    private async Task<string> CreateOutboxSchemaAsync()
    {
        var schema = IntegrationFixture.NewSchemaName();
        await fixture.CreateSchemaAsync(schema);
        await fixture.CreateOutboxMessagesTableAsync(schema);
        return schema;
    }

    private (OutboxSlotReader Reader, RecordingLogger<OutboxSlotReader> Logger, Channel<OutboxMessage> Channel, CancellationTokenSource Cts, Task Reading) StartReader(
        string schema,
        string publicationName,
        string slotName)
    {
        var logger = new RecordingLogger<OutboxSlotReader>();
        var reader = new OutboxSlotReader(
            fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection),
            fixture.BuildOptions<ReplicationOptions>(
                ReplicationOptions.ConfigurationSection,
                (ReplicationOptions.ConfigurationSection + ":SlotName", slotName),
                (ReplicationOptions.ConfigurationSection + ":PublicationName", publicationName)),
            fixture.BuildOptions<OutboxStorageOptions>(
                OutboxStorageOptions.ConfigurationSection,
                (OutboxStorageOptions.ConfigurationSection + ":Schema", $" {schema.ToUpperInvariant()} "),
                (OutboxStorageOptions.ConfigurationSection + ":Table", "Messages")),
            new OutboxMetrics(new CollectorRegistry()),
            logger);

        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16) { SingleWriter = true });
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var reading = reader.ReadAsync(channel.Writer, cts.Token);
        return (reader, logger, channel, cts, reading);
    }

    private static async Task<List<OutboxMessage>> ReadMessagesAsync(
        Channel<OutboxMessage> channel,
        int expected,
        CancellationToken cancellationToken)
    {
        var messages = new List<OutboxMessage>();
        while (messages.Count < expected)
        {
            while (messages.Count < expected && channel.Reader.TryRead(out var message))
            {
                messages.Add(message);
            }

            if (messages.Count < expected)
            {
                await channel.Reader.WaitToReadAsync(cancellationToken);
            }
        }

        return messages;
    }

    private static async Task<bool> WaitForAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(100);
        }

        return condition();
    }

    private static async Task SwallowReadingCompletionAsync(Task reading)
    {
        try
        {
            await reading;
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception)
        {
        }
    }
}
