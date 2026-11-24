using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Partitions;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Redundancy;
using CdcOutbox.Core.Replication;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Prometheus;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class PartitionMaintenanceAndOptionsTests
{
    [Fact]
    public void StorageValidator_WalMessagesEmptyPrefix_Fails()
    {
        var validator = new OutboxStorageOptionsValidator();
        var options = new OutboxStorageOptions
        {
            Mode = OutboxStorageMode.WalMessages,
            WalMessagePrefix = " ",
        };

        validator.Validate(null, options).Failed.ShouldBeTrue();
    }

    [Fact]
    public void StorageValidator_UnsafeTableIdentifier_Fails()
    {
        var validator = new OutboxStorageOptionsValidator();
        var options = new OutboxStorageOptions
        {
            Mode = OutboxStorageMode.PartitionedTable,
            Schema = "outbox",
            Table = "messages; DROP TABLE users",
        };

        validator.Validate(null, options).Failed.ShouldBeTrue();
    }

    [Fact]
    public void PublicationVerifier_WalMessagesMode_AllowsEmptyPublication()
    {
        var emptyExisting = new PublicationState(
            Exists: true,
            PublishesInserts: false,
            PublishesUpdates: false,
            PublishesDeletes: false,
            PublishesTruncates: false,
            ContainsOutboxTable: false);

        PublicationVerifier.FindPublicationIssue(
                emptyExisting, "outbox_wal_pub", "outbox", "messages", OutboxStorageMode.WalMessages)
            .ShouldBeNull();

        var missing = emptyExisting with { Exists = false };
        PublicationVerifier.FindPublicationIssue(
                missing, "outbox_wal_pub", "outbox", "messages", OutboxStorageMode.WalMessages)
            .ShouldNotBeNull();
    }

    [Fact]
    public void PublicationVerifier_PartitionedTableMode_RequiresPublishViaPartitionRoot()
    {
        var withoutViaRoot = new PublicationState(true, true, false, false, false, true, PublishViaPartitionRoot: false);
        var withViaRoot = withoutViaRoot with { PublishViaPartitionRoot = true };

        var issue = PublicationVerifier.FindPublicationIssue(
            withoutViaRoot, "outbox_part_pub", "outbox", "messages_partitioned", OutboxStorageMode.PartitionedTable);
        issue.ShouldNotBeNull();
        issue.ShouldContain("publish_via_partition_root");

        PublicationVerifier.FindPublicationIssue(
                withViaRoot, "outbox_part_pub", "outbox", "messages_partitioned", OutboxStorageMode.PartitionedTable)
            .ShouldBeNull();
    }

    [Fact]
    public void SlotCatalogEntry_LostOrInvalidationReason_DetectsInvalidatedSlot()
    {
        var healthy = new SlotCatalogEntry("pgoutput", true, "reserved", null);
        healthy.IsInvalidated.ShouldBeFalse();
        OutboxSlotProvisioner.FindExistingSlotCatalogIssue("slot1", healthy).ShouldBeNull();

        var lost = new SlotCatalogEntry("pgoutput", true, "lost", "wal_removed");
        lost.IsInvalidated.ShouldBeTrue();
        var issue = OutboxSlotProvisioner.FindExistingSlotCatalogIssue("slot1", lost);
        issue.ShouldNotBeNull();
        issue.ShouldContain("инвалидирован");
    }

    [Fact]
    public void ReplicationProgress_IdleLsn_NeverOvertakesUndeliveredQueuedMessages()
    {
        var progress = new ReplicationProgress();
        progress.MarkQueuedMessageLsn(500);
        progress.MarkIdleLsn(900);

        // сообщения до 500 ещё не доставлены — IdleLsn игнорируется
        progress.EffectiveDeliveredPosition.ShouldBe(0UL);

        progress.MarkDelivered(500);
        progress.MarkIdleLsn(900);

        // все сообщения доставлены — можно безопасно продвигать до 900
        progress.EffectiveDeliveredPosition.ShouldBe(900UL);
    }

    [Fact]
    public async Task MaintenanceWorker_RunsOnStartAndAlignedAtHH05_UpdatesMetricsAndGuard()
    {
        var time = new ManualTimeProvider(new DateTimeOffset(2026, 11, 24, 17, 2, 0, TimeSpan.Zero));
        var fakeRegistry = new FakePartitionRegistryClient();
        var progress = new ReplicationProgress();
        progress.MarkCheckpointed(0x1000, new DateTime(2026, 11, 24, 17, 1, 0, DateTimeKind.Utc));
        progress.MarkDeliveredCreatedAt(new DateTime(2026, 11, 24, 17, 0, 30, DateTimeKind.Utc));

        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, "test_slot");
        var worker = new OutboxPartitionMaintenanceWorker(
            fakeRegistry,
            progress,
            time,
            metrics,
            Options.Create(new OutboxStorageOptions { Mode = OutboxStorageMode.PartitionedTable }),
            Options.Create(new PartitionMaintenanceOptions { PrecreateHours = 48, RunAtMinuteOfHour = 5 }),
            NullLogger<OutboxPartitionMaintenanceWorker>.Instance);

        using var cts = new CancellationTokenSource();
        var runTask = worker.RunAsync(cts.Token);

        // 1. Немедленный прогон при старте (17:02:00Z) и постановка таймера на 17:05:00Z
        (await TestWait.WaitForAsync(
            () => fakeRegistry.EnsureCalls == 1 && time.CreatedTimerCount == 1,
            TimeSpan.FromSeconds(5))).ShouldBeTrue();
        fakeRegistry.DropCalls.ShouldBe(1);

        // 2. Продвигаем до 17:04:59Z — второго запуска ещё нет
        time.Advance(TimeSpan.FromMinutes(2) + TimeSpan.FromSeconds(59));
        await Task.Delay(50);
        fakeRegistry.EnsureCalls.ShouldBe(1);

        // 3. Ещё +1 секунда → ровно 17:05:00Z → срабатывает плановый запуск №2 и таймер на 18:05:00Z
        time.Advance(TimeSpan.FromSeconds(1));
        (await TestWait.WaitForAsync(
            () => fakeRegistry.EnsureCalls == 2 && time.CreatedTimerCount == 2,
            TimeSpan.FromSeconds(5))).ShouldBeTrue();

        // 4. Ещё +1 час → 18:05:00Z → срабатывает плановый запуск №3 и таймер на 19:05:00Z
        time.Advance(TimeSpan.FromHours(1));
        (await TestWait.WaitForAsync(
            () => fakeRegistry.EnsureCalls == 3 && time.CreatedTimerCount == 3,
            TimeSpan.FromSeconds(5))).ShouldBeTrue();

        await cts.CancelAsync();
        await runTask;

        var exported = await MetricsExport.ExportAsync(registry);
        exported.ShouldContain("outbox_future_partitions_count{slot=\"test_slot\"} 48");
        exported.ShouldContain("outbox_partitions_created_total{slot=\"test_slot\"} 49");
        exported.ShouldContain("outbox_partitions_dropped_total{slot=\"test_slot\"} 3");
    }

    [Theory]
    [InlineData(OutboxStorageMode.PartitionedTable, true, true)]
    [InlineData(OutboxStorageMode.PartitionedTable, false, false)]
    [InlineData(OutboxStorageMode.Table, true, false)]
    [InlineData(OutboxStorageMode.WalMessages, true, false)]
    public async Task OutboxProcessor_MidRunSlotInvalidation_TriggersAutoRecoveryOnlyWhenEnabledForPartitions(
        OutboxStorageMode mode,
        bool autoRecover,
        bool expectsSlotRecoveryException)
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, "test_slot");
        var processorOptions = Options.Create(new ProcessorOptions { RetryDelaySeconds = 1 });
        var producer = new FakeProducer();
        var progress = new ReplicationProgress();
        var publisher = new OutboxPublisher(
            producer,
            processorOptions,
            TimeProvider.System,
            metrics,
            progress,
            new CheckpointStore(
                producer,
                Options.Create(new KafkaOptions()),
                Options.Create(new ReplicationOptions { SlotName = "test_slot" }),
                TimeProvider.System,
                NullLogger<CheckpointStore>.Instance),
            NullLogger<OutboxPublisher>.Instance);
        var reader = new FailingSlotReader(new PostgresException(
            "logical replication slot \"test_slot\" has been invalidated due to WAL removal",
            "ERROR",
            "ERROR",
            PostgresErrorCodes.ObjectNotInPrerequisiteState));

        var processor = new OutboxProcessor(
            reader,
            publisher,
            processorOptions,
            metrics,
            NullLogger<OutboxProcessor>.Instance,
            partitionMaintenance: null,
            storageOptions: Options.Create(new OutboxStorageOptions { Mode = mode }),
            partitionOptions: Options.Create(new PartitionMaintenanceOptions { AutoRecoverFromPartitions = autoRecover }));

        using var cts = new CancellationTokenSource();
        if (expectsSlotRecoveryException)
        {
            await TestAssert.AssertThrowsAsync<SlotRecoveryRequiredException>(processor.RunAsync(cts.Token));
        }
        else
        {
            await TestAssert.AssertThrowsAsync<InvalidOperationException>(processor.RunAsync(cts.Token));
        }
    }

    [Fact]
    public async Task LeadershipCoordinator_MidRunSlotRecoveryRequired_RestartsTermWithFreshProducerAndRunsRecoveryChecks()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, "it_slot");
        var processorOptions = Options.Create(new ProcessorOptions { RetryDelaySeconds = 1 });
        var storageOptions = Options.Create(new OutboxStorageOptions { Mode = OutboxStorageMode.PartitionedTable });
        var partitionOptions = Options.Create(new PartitionMaintenanceOptions { AutoRecoverFromPartitions = true });
        var producers = new List<FakeProducer>();
        var checks = new CountingStartupChecks();
        var reader = new RecoveringSlotReader();

        var services = new ServiceCollection();
        services.AddScoped<IProducer<byte[], byte[]>>(_ =>
        {
            var producer = new FakeProducer();
            producers.Add(producer);
            return producer;
        });
        services.AddScoped<ReplicationProgress>();
        services.AddScoped(sp => new CheckpointStore(
            sp.GetRequiredService<IProducer<byte[], byte[]>>(),
            Options.Create(new KafkaOptions()),
            Options.Create(new ReplicationOptions { SlotName = "it_slot" }),
            TimeProvider.System,
            NullLogger<CheckpointStore>.Instance));
        services.AddScoped<IOutboxStartupChecks>(_ => checks);
        services.AddScoped(sp => new OutboxPublisher(
            sp.GetRequiredService<IProducer<byte[], byte[]>>(),
            processorOptions,
            TimeProvider.System,
            metrics,
            sp.GetRequiredService<ReplicationProgress>(),
            sp.GetRequiredService<CheckpointStore>(),
            NullLogger<OutboxPublisher>.Instance));
        services.AddScoped(sp => new OutboxProcessor(
            reader,
            sp.GetRequiredService<OutboxPublisher>(),
            processorOptions,
            metrics,
            NullLogger<OutboxProcessor>.Instance,
            partitionMaintenance: null,
            storageOptions: storageOptions,
            partitionOptions: partitionOptions));

        using var provider = services.BuildServiceProvider();
        var coordinator = new LeadershipCoordinator(
            provider.GetRequiredService<IServiceScopeFactory>(),
            processorOptions,
            metrics,
            NullLogger<LeadershipCoordinator>.Instance);

        coordinator.HandlePartitionsAssigned([new TopicPartition("cdc-checkpoints", 0)], CancellationToken.None);

        // Первая сессия ридера падает с инвалидацией слота → координатор перезапускает срок лидерства,
        // вызывает стартовые проверки (авто-восстановление) второй раз с новым продюсером и входит в штатную работу
        var recovered = await TestWait.WaitForAsync(
            () => checks.Invocations == 2 && reader.Held.Task.IsCompleted && coordinator.IsLeading,
            TimeSpan.FromSeconds(10));

        recovered.ShouldBeTrue();
        coordinator.Fatal.IsCompleted.ShouldBeFalse();
        producers.Count.ShouldBe(2);
        producers[0].Disposed.ShouldBeTrue();
        producers[1].Disposed.ShouldBeFalse();

        coordinator.HandlePartitionsRevoked();
        coordinator.IsLeading.ShouldBeFalse();
    }

    private sealed class FailingSlotReader(Exception failure) : IOutboxSlotReader
    {
        public Task ReadAsync(ChannelWriter<OutboxMessage> output, CancellationToken cancellationToken) =>
            Task.FromException(failure);
    }

    private sealed class CountingStartupChecks : IOutboxStartupChecks
    {
        public int Invocations { get; private set; }

        public Task RunAllAsync(CancellationToken cancellationToken)
        {
            Invocations++;
            return Task.CompletedTask;
        }
    }

    private sealed class RecoveringSlotReader : IOutboxSlotReader
    {
        private int _calls;

        public TaskCompletionSource Held { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task ReadAsync(ChannelWriter<OutboxMessage> output, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _calls) == 1)
            {
                throw new PostgresException(
                    "logical replication slot \"it_slot\" has been invalidated due to WAL removal",
                    "ERROR",
                    "ERROR",
                    PostgresErrorCodes.ObjectNotInPrerequisiteState);
            }

            Held.TrySetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
    }

    private sealed class FakePartitionRegistryClient : IPartitionRegistryClient
    {
        public int EnsureCalls { get; private set; }

        public int DropCalls { get; private set; }

        public Task<long> GetSystemIdentifierAsync(CancellationToken cancellationToken) => Task.FromResult(123456789L);

        public Task<PartitionEnsureResult> EnsurePartitionsAsync(DateTimeOffset nowUtc, CancellationToken cancellationToken)
        {
            EnsureCalls++;
            var created = EnsureCalls == 1 ? 49 : 0;
            return Task.FromResult(new PartitionEnsureResult(created, 48));
        }

        public Task<int> SealClosedPartitionsAsync(DateTimeOffset nowUtc, CancellationToken cancellationToken) =>
            Task.FromResult(1);

        public Task<PartitionGuardState?> ReadGuardStateAsync(CancellationToken cancellationToken) =>
            Task.FromResult<PartitionGuardState?>(new PartitionGuardState(123456789L, 0x1000, null, false));

        public Task EnsureGuardInitializedAsync(
            long systemIdentifier,
            ulong? initialLsn,
            DateTime? initialCreatedAtUtc,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task UpdateDeliveryProgressAsync(
            ulong contiguousDeliveredLsn,
            DateTime? lastDeliveredCreatedAtUtc,
            long systemIdentifier,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task SetRecoveryLockAsync(
            bool inProgress,
            long systemIdentifier,
            ulong? resetContiguousLsn,
            DateTime? resetLastCreatedAtUtc,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<IReadOnlyList<string>> DropDeliveredPartitionsAsync(
            ulong safeContiguousLsn,
            long systemIdentifier,
            DateTimeOffset nowUtc,
            CancellationToken cancellationToken)
        {
            DropCalls++;
            return Task.FromResult<IReadOnlyList<string>>(new[] { $"messages_20261124_0{DropCalls}" });
        }
    }
}
