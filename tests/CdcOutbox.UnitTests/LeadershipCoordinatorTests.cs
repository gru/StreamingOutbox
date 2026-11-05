using System.Text;
using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
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

public sealed class LeadershipCoordinatorTests
{
    [Fact]
    public async Task Grant_RegistersFailover_InitsProducerBeforeChecks_RunsDelivery()
    {
        var harness = new Harness(messages: [CreateMessage(0x100)]);

        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);
        var leading = await TestWait.WaitForAsync(
            () => harness.Coordinator.IsLeading && harness.Reader.Held.Task.IsCompleted,
            TimeSpan.FromSeconds(10));

        leading.ShouldBeTrue();
        harness.Checks.Invocations.ShouldBe(1);
        harness.Producer.TransactionCalls.First().ShouldBe("init");
        var text = await MetricsExport.ExportAsync(harness.Registry);
        text.ShouldContain("outbox_failovers_total{slot=\"it_slot\"} 1");
        text.ShouldContain("outbox_processor_is_active{slot=\"it_slot\"} 1");
        // запись в канал и публикация — независимые асинхронные шаги:
        // ждём появления записей (событие + checkpoint той же транзакцией).
        // Поллинг по Count без перечисления: список пишет поток публикатора
        var delivered = await TestWait.WaitForAsync(
            () => harness.Producer.Produced.Count >= 2,
            TimeSpan.FromSeconds(10));
        delivered.ShouldBeTrue();
        harness.Producer.Produced.Count(entry => entry.Topic == "orders.events").ShouldBe(1);
        harness.Producer.Produced.Count(entry => entry.Topic == "cdc-checkpoints").ShouldBe(1);
    }

    [Fact]
    public async Task Revoke_StopsDelivery_Deactivates_WithoutReinit()
    {
        var harness = new Harness(messages: [CreateMessage(0x100)]);
        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);
        await TestWait.WaitForAsync(() => harness.Coordinator.IsLeading, TimeSpan.FromSeconds(10));

        harness.Coordinator.HandlePartitionsRevoked();

        harness.Coordinator.IsLeading.ShouldBeFalse();
        var text = await MetricsExport.ExportAsync(harness.Registry);
        text.ShouldContain("outbox_processor_is_active{slot=\"it_slot\"} 0");
        harness.Producers.Count.ShouldBe(1);
        harness.Producer.TransactionCalls.Count(call => call == "init").ShouldBe(1);
    }

    [Fact]
    public async Task Grant_AfterRevoke_RecreatesProducer()
    {
        var harness = new Harness(messages: [CreateMessage(0x100)]);
        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);
        await TestWait.WaitForAsync(() => harness.Coordinator.IsLeading, TimeSpan.FromSeconds(10));
        var firstTermProducer = harness.Producer;
        harness.Coordinator.HandlePartitionsRevoked();

        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);
        var leading = await TestWait.WaitForAsync(() => harness.Coordinator.IsLeading, TimeSpan.FromSeconds(10));
        // подсчёт после остановки доставки: запись вызовов продюсера завершена
        harness.Coordinator.HandlePartitionsRevoked();

        leading.ShouldBeTrue();
        // второй период лидерства — свежий продюсер: повторный InitTransactions на готовом
        // экземпляре бросил бы KafkaException «Operation not valid in state Ready»
        harness.Producers.Count.ShouldBe(2);
        ReferenceEquals(harness.Producer, firstTermProducer).ShouldBeFalse();
        firstTermProducer.Disposed.ShouldBeTrue();
        foreach (var producer in harness.Producers)
        {
            producer.TransactionCalls.Count(call => call == "init").ShouldBe(1);
        }

        var text = await MetricsExport.ExportAsync(harness.Registry);
        text.ShouldContain("outbox_failovers_total{slot=\"it_slot\"} 2");
    }

    [Fact]
    public async Task Grant_EmptyPartitions_Ignored()
    {
        var harness = new Harness();

        harness.Coordinator.HandlePartitionsAssigned([], CancellationToken.None);
        await Task.Delay(200);

        harness.Coordinator.IsLeading.ShouldBeFalse();
        harness.Producers.ShouldBeEmpty();
        harness.Checks.Invocations.ShouldBe(0);
    }

    [Fact]
    public async Task Revoke_InStandby_NoOp()
    {
        var harness = new Harness();

        harness.Coordinator.HandlePartitionsRevoked();
        await Task.Delay(50);

        harness.Coordinator.IsLeading.ShouldBeFalse();
    }

    [Fact]
    public async Task FencedDelivery_StopsTerm_KeepsHostAlive()
    {
        var harness = new Harness(
            messages: [CreateMessage(0x100)],
            onCommit: static () => new KafkaException(new Error(ErrorCode.ProducerFenced)));
        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);
        await TestWait.WaitForAsync(() => harness.Coordinator.IsLeading, TimeSpan.FromSeconds(10));

        var standby = await TestWait.WaitForAsync(() => !harness.Coordinator.IsLeading, TimeSpan.FromSeconds(15));

        standby.ShouldBeTrue();
        var text = await MetricsExport.ExportAsync(harness.Registry);
        text.ShouldContain("outbox_processor_is_active{slot=\"it_slot\"} 0");
        await Task.Delay(300);
        harness.Coordinator.Fatal.IsCompleted.ShouldBeFalse();
    }

    [Fact]
    public async Task TerminalDeliveryFailure_FaultsForHost()
    {
        var harness = new Harness(
            failure: new PostgresException(
                "logical replication slot \"outbox_slot\" has been invalidated due to WAL removal",
                "ERROR", "ERROR", PostgresErrorCodes.ObjectNotInPrerequisiteState));
        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);

        await TestAssert.AssertThrowsAsync<InvalidOperationException>(harness.Coordinator.Fatal);

        harness.Coordinator.IsLeading.ShouldBeTrue();
        var text = await MetricsExport.ExportAsync(harness.Registry);
        text.ShouldContain("outbox_processor_errors_total{slot=\"it_slot\"} 1");
    }

    [Fact]
    public async Task StartupChecksFailure_FaultsForHost()
    {
        var harness = new Harness();
        harness.Checks.FailureFactory = static () =>
            new StartupCheckException("Слот отсутствует, а checkpoint подтверждённо сохранён.");
        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);

        await TestAssert.AssertThrowsAsync<InvalidOperationException>(harness.Coordinator.Fatal);

        harness.Coordinator.IsLeading.ShouldBeFalse();
        // терминальное нарушение — без повторов
        harness.Checks.Invocations.ShouldBe(1);
    }

    [Fact]
    public async Task StartupChecksTransientFailure_RetriesWithFreshProducer_ThenDelivers()
    {
        var harness = new Harness(messages: [CreateMessage(0x100)], retryDelaySeconds: 1);
        var attempts = 0;
        harness.Checks.FailureFactory = () => ++attempts == 1
            ? new NpgsqlException("Exception while reading from stream")
            : null;
        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);

        var leading = await TestWait.WaitForAsync(
            () => harness.Coordinator.IsLeading && harness.Reader.Held.Task.IsCompleted,
            TimeSpan.FromSeconds(10));

        leading.ShouldBeTrue();
        harness.Checks.Invocations.ShouldBe(2);
        // каждая попытка — свежий скоуп: продюсер периода лидерства не течёт
        harness.Producers.Count.ShouldBe(2);
        harness.Producers[0].Disposed.ShouldBeTrue();
        foreach (var producer in harness.Producers)
        {
            producer.TransactionCalls.Count(call => call == "init").ShouldBe(1);
        }

        harness.Coordinator.Fatal.IsCompleted.ShouldBeFalse();
        var text = await MetricsExport.ExportAsync(harness.Registry);
        text.ShouldContain("outbox_failovers_total{slot=\"it_slot\"} 1");
        text.ShouldContain("outbox_processor_errors_total{slot=\"it_slot\"} 1");
    }

    [Fact]
    public async Task StartupChecksPersistentTransientFailure_KeepsRetrying_HostAlive()
    {
        var harness = new Harness(retryDelaySeconds: 1);
        harness.Checks.FailureFactory = static () => new NpgsqlException("Exception while reading from stream");
        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);

        var retried = await TestWait.WaitForAsync(() => harness.Checks.Invocations >= 3, TimeSpan.FromSeconds(15));

        retried.ShouldBeTrue();
        harness.Coordinator.IsLeading.ShouldBeFalse();
        harness.Coordinator.Fatal.IsCompleted.ShouldBeFalse();
        harness.Producers.ShouldAllBe(producer => producer.Disposed);
        // останавливаем фоновый цикл повторов — тест не оставляет нагрузку
        harness.Coordinator.HandlePartitionsRevoked();
    }

    [Fact]
    public async Task InitTransactionsFenced_ReturnsToStandby_WithoutFatal()
    {
        var harness = new Harness(onInit: static () => new KafkaException(new Error(ErrorCode.ProducerFenced)));
        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);
        await Task.Delay(500);

        harness.Coordinator.IsLeading.ShouldBeFalse();
        harness.Coordinator.Fatal.IsCompleted.ShouldBeFalse();
        // fencing при инициализации — до стартовых проверок, повторов нет
        harness.Checks.Invocations.ShouldBe(0);
        harness.Producers.Count.ShouldBe(1);
    }

    [Fact]
    public async Task RevokeDuringStartupRetries_StopsAttempts_WithoutDeadlock()
    {
        var harness = new Harness(retryDelaySeconds: 1);
        harness.Checks.FailureFactory = static () => new NpgsqlException("Exception while reading from stream");
        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);
        var spinning = await TestWait.WaitForAsync(() => harness.Checks.Invocations >= 3, TimeSpan.FromSeconds(15));
        spinning.ShouldBeTrue();

        harness.Coordinator.HandlePartitionsRevoked();

        harness.Coordinator.IsLeading.ShouldBeFalse();
        var stoppedAt = harness.Checks.Invocations;
        await Task.Delay(300);
        harness.Checks.Invocations.ShouldBe(stoppedAt);
        harness.Coordinator.Fatal.IsCompleted.ShouldBeFalse();
    }

    private static List<TopicPartition> Partitions() => [new TopicPartition("cdc-checkpoints", 0)];

    private static OutboxMessage CreateMessage(ulong transactionEndLsn) =>
        new()
        {
            Id = 1,
            Topic = "orders.events",
            Key = Encoding.UTF8.GetBytes("k"),
            Body = Encoding.UTF8.GetBytes("body"),
            Headers = "{}",
            CreatedAt = DateTime.UtcNow,
            TransactionEndLsn = transactionEndLsn,
        };

    private sealed class Harness
    {
        public Harness(
            OutboxMessage[]? messages = null,
            Func<Exception?>? onCommit = null,
            Func<Exception?>? onInit = null,
            int retryDelaySeconds = 1,
            Exception? failure = null)
        {
            Registry = new CollectorRegistry();
            Metrics = new OutboxMetrics(Registry, slotName: "it_slot");
            Reader = new FakeSlotReader(messages, failure);
            Checks = new RecordingStartupChecks();

            var processorOptions = Options.Create(new ProcessorOptions { RetryDelaySeconds = retryDelaySeconds });
            var services = new ServiceCollection();
            // продюсер — сервис периода лидерства, как в хосте: свежий экземпляр
            // на скоуп каждого периода лидерства (резолвят координатор и публикатор)
            services.AddScoped<IProducer<byte[], byte[]>>(_ =>
            {
                var producer = new FakeProducer { OnCommit = onCommit, OnInitTransactions = onInit };
                Producers.Add(producer);
                return producer;
            });
            services.AddScoped<ReplicationProgress>();
            services.AddScoped(sp => new CheckpointStore(
                sp.GetRequiredService<IProducer<byte[], byte[]>>(),
                Options.Create(new KafkaOptions()),
                Options.Create(new ReplicationOptions { SlotName = "it_slot" }),
                TimeProvider.System,
                NullLogger<CheckpointStore>.Instance));
            services.AddScoped<IOutboxStartupChecks>(_ => Checks);
            services.AddScoped(sp => new OutboxPublisher(
                sp.GetRequiredService<IProducer<byte[], byte[]>>(),
                processorOptions,
                TimeProvider.System,
                Metrics,
                sp.GetRequiredService<ReplicationProgress>(),
                sp.GetRequiredService<CheckpointStore>(),
                NullLogger<OutboxPublisher>.Instance));
            services.AddScoped(sp => new OutboxProcessor(
                Reader,
                sp.GetRequiredService<OutboxPublisher>(),
                processorOptions,
                Metrics,
                NullLogger<OutboxProcessor>.Instance));
            var provider = services.BuildServiceProvider();
            Coordinator = new LeadershipCoordinator(
                provider.GetRequiredService<IServiceScopeFactory>(),
                processorOptions,
                Metrics,
                NullLogger<LeadershipCoordinator>.Instance);
        }

        public List<FakeProducer> Producers { get; } = [];

        /// <summary>
        /// Продюсер последнего периода лидерства.
        /// </summary>
        public FakeProducer Producer => Producers[^1];

        public CollectorRegistry Registry { get; }

        public OutboxMetrics Metrics { get; }

        public FakeSlotReader Reader { get; }

        public RecordingStartupChecks Checks { get; }

        public LeadershipCoordinator Coordinator { get; }
    }

    private sealed class RecordingStartupChecks : IOutboxStartupChecks
    {
        public Func<Exception?>? FailureFactory { get; set; }

        public int Invocations { get; private set; }

        public Task RunAllAsync(CancellationToken cancellationToken)
        {
            Invocations++;
            return FailureFactory?.Invoke() is { } failure ? Task.FromException(failure) : Task.CompletedTask;
        }
    }

    private sealed class FakeSlotReader(OutboxMessage[]? messages = null, Exception? failure = null)
        : IOutboxSlotReader
    {
        public TaskCompletionSource Held { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Sessions { get; private set; }

        public async Task ReadAsync(ChannelWriter<OutboxMessage> output, CancellationToken cancellationToken)
        {
            Sessions++;
            if (failure is not null)
            {
                throw failure;
            }

            foreach (var message in messages ?? [])
            {
                await output.WriteAsync(message, cancellationToken);
            }

            Held.TrySetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
    }
}
