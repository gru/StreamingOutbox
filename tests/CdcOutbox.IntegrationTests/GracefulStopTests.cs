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
public sealed class GracefulStopTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task Stop_WaitsFinalCheckpointAndConfirmsSlotBeforeClosing()
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
        // каденция заведомо не срабатывает за тест: checkpoint первого батча —
        // немедленное первое сохранение, второго — только финальное при остановке
        var processorOptions = Options.Create(new ProcessorOptions
        {
            CheckpointIntervalSeconds = 3600,
            RetryDelaySeconds = 1,
            BatchSize = 500,
            QueueCapacity = 16,
            FinalCheckpointWaitSeconds = 15,
        });
        var reader = CreateReader(schema, publicationName, slotName, processorOptions, progress, metrics);
        var checkpointStore = CreateCheckpointStore(slotName, checkpointTopic);
        var publisher = new OutboxPublisher(
            fixture.Producer,
            processorOptions,
            TimeProvider.System,
            metrics,
            progress,
            checkpointStore,
            NullLogger<OutboxPublisher>.Instance);

        using var readingCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var publisherCts = new CancellationTokenSource();
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16) { SingleWriter = true });
        var reading = reader.ReadAsync(channel.Writer, readingCts.Token);
        var publishing = publisher.PublishAsync(channel.Reader, publisherCts.Token);

        // первый батч: checkpoint сохраняется немедленно и подтверждается живым циклом
        await fixture.InsertOutboxMessagesAsync(
            schema,
            new OutboxInsert(eventsTopic, new byte[] { 0x01 }, new byte[] { 0x01 }, CreatedAt: DateTime.UtcNow),
            new OutboxInsert(eventsTopic, new byte[] { 0x02 }, new byte[] { 0x02 }, CreatedAt: DateTime.UtcNow));
        fixture.ConsumeTopic(eventsTopic, expected: 2, idleWindow: TimeSpan.FromSeconds(2));
        var firstCheckpoint = await Wait.UntilCheckpointAsync(checkpointStore);
        firstCheckpoint.ShouldNotBeNull();

        // второй батч: его прогресс подтверждается только финальным сохранением
        await fixture.InsertOutboxMessagesAsync(
            schema,
            new OutboxInsert(eventsTopic, new byte[] { 0x03 }, new byte[] { 0x03 }, CreatedAt: DateTime.UtcNow));
        fixture.ConsumeTopic(eventsTopic, expected: 3, idleWindow: TimeSpan.FromSeconds(2));
        var delivered = await Wait.UntilAsync(
            async () => (await MetricsExport.ExportAsync(registry))
                .Contains($"outbox_processor_sent_messages_total{{slot=\"{slotName}\"}} 3"),
            TimeSpan.FromSeconds(30));
        delivered.ShouldBeTrue();

        // отмена ридера — штатная остановка: канал завершает ридер, публикатор дренирует
        // и сохраняет финальный checkpoint, ридер подтверждает позицию серверу
        // и только затем закрывает соединение
        await readingCts.CancelAsync();
        await Wait.SwallowAsync(reading);
        await publishing;
        await publisherCts.CancelAsync();

        var finalCheckpoint = checkpointStore.ReadLatest(CancellationToken.None);
        finalCheckpoint.ShouldNotBeNull();
        finalCheckpoint.Value.ShouldBeGreaterThan(firstCheckpoint.Value);

        var confirmed = await Wait.UntilAsync(async () =>
        {
            var state = (await fixture.GetSlotsAsync(slotName)).Single();
            return LsnText.TryParse(state.ConfirmedFlushLsn) is { } lsn && lsn >= finalCheckpoint;
        }, TimeSpan.FromSeconds(30));
        confirmed.ShouldBeTrue();
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
            fixture.Producer,
            fixture.BuildOptions<KafkaOptions>(
                KafkaOptions.ConfigurationSection,
                (KafkaOptions.ConfigurationSection + ":CheckpointTopic", checkpointTopic)),
            Options.Create(new ReplicationOptions { SlotName = slotName }),
            TimeProvider.System,
            NullLogger<CheckpointStore>.Instance);
}
