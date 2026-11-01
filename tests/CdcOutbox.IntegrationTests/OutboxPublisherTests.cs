using System.Text;
using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Replication;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Prometheus;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

[Collection("integration")]
public sealed class OutboxPublisherTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task PublishPipeline_DeliversSlotMessagesToKafka()
    {
        var schema = await CreateOutboxSchemaAsync();
        var publicationName = IntegrationFixture.NewName();
        await fixture.CreatePublicationAsync(publicationName, schema, new[] { "messages" });
        var slotName = IntegrationFixture.NewName();
        await fixture.CreateLogicalSlotAsync(slotName);
        var topic = IntegrationFixture.NewName();
        await CreateTopicAsync(topic, partitions: 3);

        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry);
        var reader = new OutboxSlotReader(
            fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection),
            fixture.BuildOptions<ReplicationOptions>(
                ReplicationOptions.ConfigurationSection,
                (ReplicationOptions.ConfigurationSection + ":SlotName", slotName),
                (ReplicationOptions.ConfigurationSection + ":PublicationName", publicationName)),
            fixture.BuildOptions<OutboxStorageOptions>(
                OutboxStorageOptions.ConfigurationSection,
                (OutboxStorageOptions.ConfigurationSection + ":Schema", schema),
                (OutboxStorageOptions.ConfigurationSection + ":Table", "messages")),
            metrics,
            NullLogger<OutboxSlotReader>.Instance);
        var publisher = new OutboxPublisher(
            CreateProducer(),
            Options.Create(new ProcessorOptions { BatchSize = 16, RetryDelaySeconds = 1 }),
            TimeProvider.System,
            metrics,
            NullLogger<OutboxPublisher>.Instance);

        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16) { SingleWriter = true });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var publishing = publisher.PublishAsync(channel.Reader, cts.Token);
        var reading = reader.ReadAsync(channel.Writer, cts.Token);

        var createdAt = new DateTime(2026, 10, 17, 6, 42, 11, 123, 456, DateTimeKind.Utc);
        await fixture.InsertOutboxMessagesAsync(
            schema,
            new OutboxInsert(topic, [0x0a, 0x0b, 0x0c], [0xde, 0xad, 0xbe, 0xef], CreatedAt: createdAt),
            new OutboxInsert(topic, [0x01, 0x02], [0x03, 0x04], CreatedAt: createdAt),
            new OutboxInsert(topic, [0x05], [0x06], CreatedAt: createdAt));
        await fixture.ClearOutboxMessagesAsync(schema);

        var consumed = fixture.ConsumeTopic(topic, expected: 3);
        consumed.Count.ShouldBe(3);
        consumed.ShouldAllBe(result => result.Topic == topic);
        consumed.Select(result => result.Message.Key)
            .ShouldContain(key => key.SequenceEqual(new byte[] { 0x0a, 0x0b, 0x0c }));
        consumed.Select(result => result.Message.Value)
            .ShouldContain(value => value.SequenceEqual(new byte[] { 0xde, 0xad, 0xbe, 0xef }));

        await cts.CancelAsync();
        await SwallowAsync(publishing);
        await SwallowAsync(reading);

        var text = await WaitForExportAsync(registry, "outbox_processor_sent_messages_total{partition=\"0\"} 3");
        text.ShouldContain("outbox_processor_pending_events_current 0");
        text.ShouldContain("outbox_processor_read_messages_current{partition=\"0\"} 3");
    }

    [Fact]
    public async Task Publish_RoutesExplicitPartitionAndMapsHeaders()
    {
        var topic = IntegrationFixture.NewName();
        await CreateTopicAsync(topic, partitions: 3);
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry);
        var publisher = new OutboxPublisher(
            CreateProducer(),
            Options.Create(new ProcessorOptions { BatchSize = 16, RetryDelaySeconds = 1 }),
            TimeProvider.System,
            metrics,
            NullLogger<OutboxPublisher>.Instance);
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(new OutboxMessage
        {
            Id = 1,
            Topic = topic,
            Key = new byte[] { 1 },
            Body = new byte[] { 2 },
            Headers = "{\"trace-id\":\"abc-123\"}",
            Partition = 1,
            CreatedAt = DateTime.UtcNow,
        });
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await publisher.PublishAsync(channel.Reader, cts.Token);

        var consumed = fixture.ConsumeTopic(topic, expected: 1, idleWindow: TimeSpan.FromSeconds(3));
        consumed.ShouldHaveSingleItem();
        var result = consumed[0];
        result.Partition.Value.ShouldBe(1);
        Encoding.UTF8.GetString(result.Message.Headers.GetLastBytes("trace-id")).ShouldBe("abc-123");
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_sent_messages_total{partition=\"0\"} 1");
    }

    private IProducer<byte[], byte[]> CreateProducer()
    {
        var kafka = fixture.BuildOptions<KafkaOptions>(KafkaOptions.ConfigurationSection).Value;
        return new ProducerBuilder<byte[], byte[]>(OutboxProducerConfig.Build(kafka)).Build();
    }

    private async Task<string> CreateOutboxSchemaAsync()
    {
        var schema = IntegrationFixture.NewSchemaName();
        await fixture.CreateSchemaAsync(schema);
        await fixture.CreateOutboxMessagesTableAsync(schema);
        return schema;
    }

    private async Task CreateTopicAsync(string topic, int partitions)
    {
        await fixture.AdminClient.CreateTopicsAsync(
            new[] { new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 } });
        for (var attempt = 0; attempt < 60; attempt++)
        {
            try
            {
                var metadata = fixture.AdminClient.GetMetadata(topic, TimeSpan.FromSeconds(5));
                var found = metadata.Topics.FirstOrDefault(item => item.Topic == topic);
                if (found is not null && found.Partitions.All(partition => partition.Leader >= 0))
                {
                    return;
                }
            }
            catch (KafkaException)
            {
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }

        throw new TimeoutException($"Topic '{topic}' не получил лидера партиций за 30 с после создания.");
    }

    private static async Task<string> WaitForExportAsync(CollectorRegistry registry, string expected, TimeSpan? timeout = null)
    {
        var text = string.Empty;
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (DateTime.UtcNow < deadline)
        {
            text = await MetricsExport.ExportAsync(registry);
            if (text.Contains(expected, StringComparison.Ordinal))
            {
                return text;
            }

            await Task.Delay(100);
        }

        return text;
    }

    private static async Task SwallowAsync(Task task)
    {
        try
        {
            await task;
        }
        catch (Exception)
        {
        }
    }
}
