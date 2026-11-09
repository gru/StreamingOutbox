using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Prometheus;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

[Collection("integration")]
public sealed class RecoveryModeTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task DroppedSlot_WithoutRecovery_Terminal_WithRecovery_RecreatedAndDeliveryContinues()
    {
        var (schema, publicationName, slotName, checkpointTopic, eventsTopic) = await CreateWorldAsync();
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: slotName);
        var checkpointStore = CreateCheckpointStore(slotName, checkpointTopic);

        // живой конвейер доставляет пачку — checkpoint подтверждённо сохранён
        var checkpoint = await DeliverAsync(
            schema, publicationName, slotName, eventsTopic, checkpointStore, metrics,
            checkpointAfter: null, expectedTotal: 2,
            new OutboxInsert(eventsTopic, new byte[] { 0x01 }, new byte[] { 0x01 }, CreatedAt: DateTime.UtcNow),
            new OutboxInsert(eventsTopic, new byte[] { 0x02 }, new byte[] { 0x02 }, CreatedAt: DateTime.UtcNow));
        checkpoint.ShouldNotBeNull();

        // дроп слота: обычный режим — терминальная ошибка, слот не пересоздаётся
        await fixture.ExecuteAsync("SELECT pg_drop_replication_slot($1)", slotName);
        (await fixture.GetSlotsAsync(slotName)).ShouldBeEmpty();
        var normalError = await Should.ThrowAsync<InvalidOperationException>(
            () => CreateStartupChecks(schema, publicationName, slotName, checkpointTopic, checkpointStore, metrics, recoveryMode: false)
                .RunAllAsync(CancellationToken.None));
        normalError.Message.ShouldContain("автопересоздание запрещено");
        (await fixture.GetSlotsAsync(slotName)).ShouldBeEmpty();

        // recovery-старт: слот пересоздан, checkpoint переписан на позицию нового слота
        await CreateStartupChecks(schema, publicationName, slotName, checkpointTopic, checkpointStore, metrics, recoveryMode: true)
            .RunAllAsync(CancellationToken.None);
        var slot = (await fixture.GetSlotsAsync(slotName)).Single();
        slot.Plugin.ShouldBe("pgoutput");
        var newFlush = LsnText.TryParse(slot.ConfirmedFlushLsn);
        newFlush.ShouldNotBeNull();
        var recovered = checkpointStore.ReadLatest(CancellationToken.None);
        recovered.ShouldNotBeNull();
        recovered.Value.ShouldBe(newFlush.Value);
        // доставка продолжается с пересозданного слота: свежая пачка доходит
        var delivered = await DeliverAsync(
            schema, publicationName, slotName, eventsTopic, checkpointStore, metrics,
            checkpointAfter: recovered, expectedTotal: 3,
            new OutboxInsert(eventsTopic, new byte[] { 0x03 }, new byte[] { 0x03 }, CreatedAt: DateTime.UtcNow));
        delivered.ShouldNotBeNull();
        delivered.Value.ShouldBeGreaterThan(recovered.Value);
    }

    [Fact]
    public async Task CheckpointAheadOfFlush_RecoveryRewritesCheckpointDown_ThenNoOp()
    {
        var (schema, publicationName, slotName, checkpointTopic, eventsTopic) = await CreateWorldAsync();
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: slotName);
        var checkpointStore = CreateCheckpointStore(slotName, checkpointTopic);

        // согласованное состояние: доставка сохранила checkpoint и подтвердила слот
        var checkpoint = await DeliverAsync(
            schema, publicationName, slotName, eventsTopic, checkpointStore, metrics,
            checkpointAfter: null, expectedTotal: 1,
            new OutboxInsert(eventsTopic, new byte[] { 0x01 }, new byte[] { 0x01 }, CreatedAt: DateTime.UtcNow));
        checkpoint.ShouldNotBeNull();
        var flush = await Wait.UntilAsync(async () =>
        {
            var state = (await fixture.GetSlotsAsync(slotName)).Single();
            var parsed = LsnText.TryParse(state.ConfirmedFlushLsn);
            return parsed is not null && parsed >= checkpoint.Value;
        }, TimeSpan.FromSeconds(30));
        flush.ShouldBeTrue();
        var confirmedFlush = LsnText.TryParse((await fixture.GetSlotsAsync(slotName)).Single().ConfirmedFlushLsn)!.Value;

        // рассогласование: checkpoint опережает confirmed_flush слота
        await checkpointStore.SaveTransactionAsync(confirmedFlush + 0x10000, CancellationToken.None);

        // recovery переписывает checkpoint вниз на позицию слота
        await CreateStartupChecks(schema, publicationName, slotName, checkpointTopic, checkpointStore, metrics, recoveryMode: true)
            .RunAllAsync(CancellationToken.None);
        checkpointStore.ReadLatest(CancellationToken.None).ShouldBe(confirmedFlush);

        // повторный recovery при согласованном состоянии — no-op: не бросает и не двигает checkpoint
        await CreateStartupChecks(schema, publicationName, slotName, checkpointTopic, checkpointStore, metrics, recoveryMode: true)
            .RunAllAsync(CancellationToken.None);
        checkpointStore.ReadLatest(CancellationToken.None).ShouldBe(confirmedFlush);
    }

    [Fact]
    public async Task CheckpointAheadOfFlush_WithoutRecovery_Continues()
    {
        var (schema, publicationName, slotName, checkpointTopic, eventsTopic) = await CreateWorldAsync();
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: slotName);
        var checkpointStore = CreateCheckpointStore(slotName, checkpointTopic);

        // согласованное состояние: доставка сохранила checkpoint и подтвердила слот
        var checkpoint = await DeliverAsync(
            schema, publicationName, slotName, eventsTopic, checkpointStore, metrics,
            checkpointAfter: null, expectedTotal: 1,
            new OutboxInsert(eventsTopic, new byte[] { 0x01 }, new byte[] { 0x01 }, CreatedAt: DateTime.UtcNow));
        checkpoint.ShouldNotBeNull();
        var flush = await Wait.UntilAsync(async () =>
        {
            var state = (await fixture.GetSlotsAsync(slotName)).Single();
            var parsed = LsnText.TryParse(state.ConfirmedFlushLsn);
            return parsed is not null && parsed >= checkpoint.Value;
        }, TimeSpan.FromSeconds(30));
        flush.ShouldBeTrue();
        var confirmedFlush = LsnText.TryParse((await fixture.GetSlotsAsync(slotName)).Single().ConfirmedFlushLsn)!.Value;

        // рассогласование: checkpoint опережает confirmed_flush слота (как опоздавший
        // commit финального checkpoint при остановке) — обычный режим продолжает
        // с позиции слота, без терминальной ошибки и без Recovery Mode
        await checkpointStore.SaveTransactionAsync(confirmedFlush + 0x10000, CancellationToken.None);

        await CreateStartupChecks(schema, publicationName, slotName, checkpointTopic, checkpointStore, metrics, recoveryMode: false)
            .RunAllAsync(CancellationToken.None);

        // checkpoint не переписывается — запись выровняет первый доставленный батч
        checkpointStore.ReadLatest(CancellationToken.None).ShouldBe(confirmedFlush + 0x10000);
    }

    [Fact]
    public async Task FlushAheadOfCheckpoint_WithoutRecovery_AdoptsSlotPositionAsCheckpoint()
    {
        var (schema, publicationName, slotName, checkpointTopic, eventsTopic) = await CreateWorldAsync();
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: slotName);
        var checkpointStore = CreateCheckpointStore(slotName, checkpointTopic);

        // согласованное состояние: доставка сохранила checkpoint и подтвердила слот
        var checkpoint = await DeliverAsync(
            schema, publicationName, slotName, eventsTopic, checkpointStore, metrics,
            checkpointAfter: null, expectedTotal: 1,
            new OutboxInsert(eventsTopic, new byte[] { 0x01 }, new byte[] { 0x01 }, CreatedAt: DateTime.UtcNow));
        checkpoint.ShouldNotBeNull();
        var flush = await Wait.UntilAsync(async () =>
        {
            var state = (await fixture.GetSlotsAsync(slotName)).Single();
            var parsed = LsnText.TryParse(state.ConfirmedFlushLsn);
            return parsed is not null && parsed >= checkpoint.Value;
        }, TimeSpan.FromSeconds(30));
        flush.ShouldBeTrue();
        var slotState = (await fixture.GetSlotsAsync(slotName)).Single();
        var confirmedFlush = LsnText.TryParse(slotState.ConfirmedFlushLsn)!.Value;

        // рассогласование: confirmed_flush слота опережает checkpoint (как внешнее
        // продвижение слота) — обычный режим принимает позицию слота как checkpoint
        // до начала чтения, помечает пропуск WARN'ом и метрикой
        await checkpointStore.SaveTransactionAsync(confirmedFlush - 0x10000, CancellationToken.None);
        var logger = new RecordingLogger<OutboxStartupChecks>();

        await CreateStartupChecks(
                schema, publicationName, slotName, checkpointTopic, checkpointStore, metrics,
                recoveryMode: false, logger: logger)
            .RunAllAsync(CancellationToken.None);

        // checkpoint запечатан позицией слота — последующие старты согласуются штатно
        checkpointStore.ReadLatest(CancellationToken.None).ShouldBe(confirmedFlush);
        var slots = await fixture.GetSlotsAsync(slotName);
        slots.ShouldNotBeEmpty();
        logger.Entries.ShouldContain(entry =>
            entry.Level == LogLevel.Warning && entry.Message.Contains("пропускаются"));
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain($"outbox_checkpoint_skips_total{{slot=\"{slotName}\"}} 1");
    }

    private async Task<(string Schema, string Publication, string Slot, string CheckpointTopic, string EventsTopic)> CreateWorldAsync()
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
        return (schema, publicationName, slotName, checkpointTopic, eventsTopic);
    }

    private async Task<ulong?> DeliverAsync(
        string schema,
        string publicationName,
        string slotName,
        string eventsTopic,
        CheckpointStore checkpointStore,
        OutboxMetrics metrics,
        ulong? checkpointAfter,
        int expectedTotal,
        params OutboxInsert[] batch)
    {
        var progress = new ReplicationProgress();
        var processorOptions = Options.Create(new ProcessorOptions
        {
            CheckpointIntervalSeconds = 1,
            RetryDelaySeconds = 1,
            QueueCapacity = 16,
        });
        var reader = CreateReader(schema, publicationName, slotName, processorOptions, progress, metrics);
        var publisher = CreatePublisher(processorOptions, progress, checkpointStore, metrics);

        using var readingCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var publisherCts = new CancellationTokenSource();
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16) { SingleWriter = true });
        var reading = reader.ReadAsync(channel.Writer, readingCts.Token);
        var publishing = publisher.PublishAsync(channel.Reader, publisherCts.Token);

        await fixture.InsertOutboxMessagesAsync(schema, batch);
        fixture.ConsumeTopic(eventsTopic, expected: expectedTotal, idleWindow: TimeSpan.FromSeconds(2));
        var checkpoint = await Wait.UntilCheckpointAsync(checkpointStore, checkpointAfter);

        await readingCts.CancelAsync();
        await Wait.SwallowAsync(reading);
        await publishing;
        await publisherCts.CancelAsync();
        return checkpoint;
    }

    private OutboxStartupChecks CreateStartupChecks(
        string schema,
        string publicationName,
        string slotName,
        string checkpointTopic,
        CheckpointStore checkpointStore,
        OutboxMetrics metrics,
        bool recoveryMode,
        ILogger<OutboxStartupChecks>? logger = null)
    {
        var replication = fixture.BuildOptions<ReplicationOptions>(
            ReplicationOptions.ConfigurationSection,
            (ReplicationOptions.ConfigurationSection + ":SlotName", slotName),
            (ReplicationOptions.ConfigurationSection + ":PublicationName", publicationName));
        return new OutboxStartupChecks(
            new OutboxSlotProvisioner(
                fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection),
                replication,
                NullLogger<OutboxSlotProvisioner>.Instance),
            new PublicationVerifier(
                fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection),
                replication,
                fixture.BuildOptions<OutboxStorageOptions>(
                    OutboxStorageOptions.ConfigurationSection,
                    (OutboxStorageOptions.ConfigurationSection + ":Schema", schema),
                    (OutboxStorageOptions.ConfigurationSection + ":Table", "messages")),
                NullLogger<PublicationVerifier>.Instance),
            new CheckpointTopicVerifier(
                fixture.AdminClient,
                fixture.BuildOptions<KafkaOptions>(
                    KafkaOptions.ConfigurationSection,
                    (KafkaOptions.ConfigurationSection + ":CheckpointTopic", checkpointTopic)),
                NullLogger<CheckpointTopicVerifier>.Instance),
            checkpointStore,
            metrics,
            Options.Create(new CdcOutboxOptions { RecoveryMode = recoveryMode }),
            logger ?? NullLogger<OutboxStartupChecks>.Instance);
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

    private OutboxPublisher CreatePublisher(
        IOptions<ProcessorOptions> processorOptions,
        ReplicationProgress progress,
        CheckpointStore checkpointStore,
        OutboxMetrics metrics) =>
        new(
            fixture.TransactionalProducer,
            processorOptions,
            TimeProvider.System,
            metrics,
            progress,
            checkpointStore,
            NullLogger<OutboxPublisher>.Instance);

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
