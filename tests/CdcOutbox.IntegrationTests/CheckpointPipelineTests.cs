using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Prometheus;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

[Collection("integration")]
public sealed class CheckpointPipelineTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task Pipeline_Delivers_SavesCheckpoint_AndAdvancesConfirmedFlush()
    {
        var schema = IntegrationFixture.NewSchemaName();
        await fixture.CreateSchemaAsync(schema);
        await fixture.CreateOutboxMessagesTableAsync(schema);
        var publicationName = IntegrationFixture.NewName();
        await fixture.CreatePublicationAsync(publicationName, schema, new[] { "messages" });
        var slotName = IntegrationFixture.NewName();
        await fixture.CreateLogicalSlotAsync(slotName);
        var checkpointTopic = IntegrationFixture.NewName();
        await fixture.CreateTopicAsync(checkpointTopic);
        var eventsTopic = IntegrationFixture.NewName();
        await fixture.CreateTopicAsync(eventsTopic, partitions: 3);

        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName);
        var progress = new ReplicationProgress();
        var processorOptions = Options.Create(new ProcessorOptions
        {
            CheckpointIntervalSeconds = 1,
            RetryDelaySeconds = 1,
            BatchSize = 500,
            QueueCapacity = 16,
        });
        var reader = CreateReader(schema, publicationName, slotName, processorOptions, progress, metrics);
        var checkpointStore = CreateCheckpointStore(slotName, checkpointTopic);
        var publisher = new OutboxPublisher(
            fixture.TransactionalProducer,
            processorOptions,
            TimeProvider.System,
            metrics,
            progress,
            checkpointStore,
            NullLogger<OutboxPublisher>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16) { SingleWriter = true });
        var reading = reader.ReadAsync(channel.Writer, cts.Token);
        var publishing = publisher.PublishAsync(channel.Reader, cts.Token);

        await fixture.InsertOutboxMessagesAsync(
            schema,
            new OutboxInsert(eventsTopic, new byte[] { 0x01 }, new byte[] { 0x01 }, CreatedAt: DateTime.UtcNow),
            new OutboxInsert(eventsTopic, new byte[] { 0x02 }, new byte[] { 0x02 }, CreatedAt: DateTime.UtcNow),
            new OutboxInsert(eventsTopic, new byte[] { 0x03 }, new byte[] { 0x03 }, CreatedAt: DateTime.UtcNow));

        var consumed = fixture.ConsumeTopic(eventsTopic, expected: 3, idleWindow: TimeSpan.FromSeconds(2));
        consumed.Count.ShouldBe(3);

        var firstCheckpoint = await Wait.UntilCheckpointAsync(checkpointStore);
        firstCheckpoint.ShouldNotBeNull();

        var confirmedAdvanced = await Wait.UntilAsync(async () =>
        {
            var confirmed = LsnText.TryParse((await fixture.GetSlotsAsync(slotName)).Single().ConfirmedFlushLsn);
            return confirmed is not null && confirmed >= firstCheckpoint;
        }, TimeSpan.FromSeconds(30));
        confirmedAdvanced.ShouldBeTrue();

        await fixture.InsertOutboxMessagesAsync(
            schema,
            new OutboxInsert(eventsTopic, new byte[] { 0x04 }, new byte[] { 0x04 }, CreatedAt: DateTime.UtcNow),
            new OutboxInsert(eventsTopic, new byte[] { 0x05 }, new byte[] { 0x05 }, CreatedAt: DateTime.UtcNow));
        fixture.ConsumeTopic(eventsTopic, expected: 5, idleWindow: TimeSpan.FromSeconds(2));

        var secondCheckpoint = await Wait.UntilCheckpointAsync(checkpointStore, greaterThan: firstCheckpoint);
        secondCheckpoint.ShouldNotBeNull();

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain($"outbox_checkpoint_age_seconds{{slot=\"{slotName}\"}}");
        text.ShouldContain($"outbox_checkpoint_lag_bytes{{slot=\"{slotName}\"}}");

        channel.Writer.Complete();
        await publishing;
        await cts.CancelAsync();
        await Wait.SwallowAsync(reading);
    }

    private OutboxSlotReader CreateReader(
        string schema,
        string publicationName,
        string slotName,
        IOptions<ProcessorOptions> processorOptions,
        ReplicationProgress progress,
        OutboxMetrics metrics) =>
        new(
            fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection),
            Options.Create(new ReplicationOptions { SlotName = slotName, PublicationName = publicationName }),
            fixture.BuildOptions<OutboxStorageOptions>(
                OutboxStorageOptions.ConfigurationSection,
                (OutboxStorageOptions.ConfigurationSection + ":Schema", schema),
                (OutboxStorageOptions.ConfigurationSection + ":Table", "messages")),
            processorOptions,
            progress,
            TimeProvider.System,
            metrics,
            NullLogger<OutboxSlotReader>.Instance);

    private CheckpointStore CreateCheckpointStore(string slotName, string checkpointTopic) =>
        new(
            fixture.TransactionalProducer,
            fixture.BuildOptions<KafkaOptions>(
                KafkaOptions.ConfigurationSection,
                (KafkaOptions.ConfigurationSection + ":CheckpointTopic", checkpointTopic)),
            Options.Create(new ReplicationOptions { SlotName = slotName }),
            TimeProvider.System,
            NullLogger<CheckpointStore>.Instance);
}
