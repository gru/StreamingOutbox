using System.Text;
using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Replication;
using Confluent.Kafka;
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
        await fixture.CreateTopicAsync(topic, partitions: 3);
        var checkpointTopic = IntegrationFixture.NewName();
        await fixture.CreateTopicAsync(checkpointTopic);

        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName);
        var progress = new ReplicationProgress();
        var processorOptions = Options.Create(new ProcessorOptions
        {
            BatchSize = 16,
            RetryDelaySeconds = 1,
            CheckpointIntervalSeconds = 1,
        });
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
            processorOptions,
            progress,
            TimeProvider.System,
            metrics,
            NullLogger<OutboxSlotReader>.Instance);
        var publisher = CreatePublisher(metrics, progress, slotName, checkpointTopic, checkpointIntervalSeconds: 1);

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

        var consumed = fixture.ConsumeTopic(topic, expected: 3);
        consumed.Count.ShouldBe(3);
        consumed.ShouldAllBe(result => result.Topic == topic);
        consumed.Select(result => result.Message.Key)
            .ShouldContain(key => key.SequenceEqual(new byte[] { 0x0a, 0x0b, 0x0c }));
        consumed.Select(result => result.Message.Value)
            .ShouldContain(value => value.SequenceEqual(new byte[] { 0xde, 0xad, 0xbe, 0xef }));

        await cts.CancelAsync();
        await Wait.SwallowAsync(publishing);
        await Wait.SwallowAsync(reading);

        var text = await WaitForExportAsync(registry, $"outbox_processor_sent_messages_total{{slot=\"{slotName}\"}} 3");
        text.ShouldContain($"outbox_processor_pending_events_current{{slot=\"{slotName}\"}} 0");
        text.ShouldContain($"outbox_processor_read_messages_current{{slot=\"{slotName}\"}} 3");
    }

    [Fact]
    public async Task Publish_RoutesExplicitPartitionAndMapsHeaders()
    {
        var topic = IntegrationFixture.NewName();
        await fixture.CreateTopicAsync(topic, partitions: 3);
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_publisher");
        var publisher = CreatePublisher(metrics, new ReplicationProgress(), "it_publisher", "cdc-checkpoints");
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
        text.ShouldContain("outbox_processor_sent_messages_total{slot=\"it_publisher\"} 1");
    }

    private OutboxPublisher CreatePublisher(
        OutboxMetrics metrics,
        ReplicationProgress progress,
        string slotName,
        string checkpointTopic,
        int checkpointIntervalSeconds = 5) =>
        new(
            CreateProducer(),
            Options.Create(new ProcessorOptions
            {
                BatchSize = 16,
                RetryDelaySeconds = 1,
                CheckpointIntervalSeconds = checkpointIntervalSeconds,
            }),
            TimeProvider.System,
            metrics,
            progress,
            new CheckpointStore(
                fixture.Producer,
                fixture.BuildOptions<KafkaOptions>(
                    KafkaOptions.ConfigurationSection,
                    (KafkaOptions.ConfigurationSection + ":CheckpointTopic", checkpointTopic)),
                Options.Create(new ReplicationOptions { SlotName = slotName }),
                TimeProvider.System,
                NullLogger<CheckpointStore>.Instance),
            NullLogger<OutboxPublisher>.Instance);

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
}
