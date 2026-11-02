using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
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

        var createdAt = new DateTime(2026, 10, 17, 6, 42, 11, 123, 456, DateTimeKind.Utc);
        await fixture.InsertOutboxMessagesAsync(
            schema,
            new OutboxInsert("orders.events", [0x0a, 0x0b, 0x0c], [0xde, 0xad, 0xbe, 0xef], Partition: 7, ProducerKey: "cluster-b", CreatedAt: createdAt),
            new OutboxInsert("orders.events", [0x01, 0x02], [0x03, 0x04], CreatedAt: createdAt),
            new OutboxInsert("billing.events", [0x05], [0x06], CreatedAt: createdAt));

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

        messages.Take(2).ShouldAllBe(message => message.TransactionEndLsn == null);
        messages[^1].TransactionEndLsn.ShouldNotBeNull().ShouldBeGreaterThan(0UL);

        await cts.CancelAsync();
        await Wait.SwallowAsync(reading);

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
        var warned = await Wait.UntilAsync(
            () => logger.Entries.Any(entry =>
                entry.Level == LogLevel.Warning && entry.Message.Contains(schema) && entry.Message.Contains("notes")),
            TimeSpan.FromSeconds(10));
        warned.ShouldBeTrue();
        channel.Reader.TryRead(out _).ShouldBeFalse();

        await fixture.ExecuteAsync($"INSERT INTO {schema}.notes VALUES (2, 'повторная чужая вставка')");
        await fixture.InsertOutboxMessagesAsync(
            schema,
            new OutboxInsert("orders.events", [0x07], [0x08], CreatedAt: new DateTime(2026, 10, 17, 6, 42, 11, DateTimeKind.Utc)));

        var messages = await ReadMessagesAsync(channel, 1, cts.Token);
        messages.ShouldHaveSingleItem().Key.ShouldBe(new byte[] { 0x07 });

        await cts.CancelAsync();
        await Wait.SwallowAsync(reading);

        logger.Entries.Count(entry => entry.Level == LogLevel.Warning && entry.Message.Contains("notes")).ShouldBe(1);
    }

    [Fact]
    public async Task Read_IdleTimeout_WarnsWithoutMessagesAndResetsOnActivity()
    {
        var schema = await CreateOutboxSchemaAsync();
        var publicationName = IntegrationFixture.NewName();
        await fixture.CreatePublicationAsync(publicationName, schema, new[] { "messages" });
        var slotName = IntegrationFixture.NewName();
        await fixture.CreateLogicalSlotAsync(slotName);

        var (_, logger, channel, cts, reading) = StartReader(schema, publicationName, slotName, idleTimeoutSeconds: 1);
        using var __ = cts;

        // тишина: предупреждение появляется по истечении интервала
        var firstWarned = await Wait.UntilAsync(
            () => CountIdleWarnings(logger) > 0,
            TimeSpan.FromSeconds(10));
        firstWarned.ShouldBeTrue();

        // новая транзакция с outbox-сообщением сбрасывает таймер простоя
        await fixture.InsertOutboxMessagesAsync(
            schema,
            new OutboxInsert("orders.events", [0x11], [0x12], CreatedAt: new DateTime(2026, 10, 17, 6, 42, 11, DateTimeKind.Utc)));
        var messages = await ReadMessagesAsync(channel, 1, cts.Token);
        messages.ShouldHaveSingleItem().Key.ShouldBe(new byte[] { 0x11 });

        var baseline = CountIdleWarnings(logger);
        await Task.Delay(500);
        CountIdleWarnings(logger).ShouldBe(baseline);

        // тишина возобновилась: предупреждение возвращается с каденсом интервала
        var resumed = await Wait.UntilAsync(
            () => CountIdleWarnings(logger) > baseline,
            TimeSpan.FromSeconds(5));
        resumed.ShouldBeTrue();

        await cts.CancelAsync();
        await Wait.SwallowAsync(reading);
    }

    private static int CountIdleWarnings(RecordingLogger<OutboxSlotReader> logger) =>
        logger.Entries.Count(entry => entry.Level == LogLevel.Warning && entry.Message.Contains("Простой потока"));

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
        string slotName,
        int idleTimeoutSeconds = 30)
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
            Options.Create(new ProcessorOptions { IdleTimeoutSeconds = idleTimeoutSeconds }),
            new ReplicationProgress(),
            TimeProvider.System,
            new OutboxMetrics(new CollectorRegistry(), slotName),
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
}
