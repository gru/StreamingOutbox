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
        harness.Producer.TransactionCalls.Count(call => call == "init").ShouldBe(1);
    }

    [Fact]
    public async Task Grant_AfterRevoke_ReinitializesProducer()
    {
        var harness = new Harness(messages: [CreateMessage(0x100)]);
        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);
        await TestWait.WaitForAsync(() => harness.Coordinator.IsLeading, TimeSpan.FromSeconds(10));
        harness.Coordinator.HandlePartitionsRevoked();

        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);
        var leading = await TestWait.WaitForAsync(() => harness.Coordinator.IsLeading, TimeSpan.FromSeconds(10));
        // подсчёт после остановки доставки: запись вызовов продюсера завершена
        harness.Coordinator.HandlePartitionsRevoked();

        leading.ShouldBeTrue();
        harness.Producer.TransactionCalls.Count(call => call == "init").ShouldBe(2);
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
        harness.Producer.TransactionCalls.ShouldBeEmpty();
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
        var harness = new Harness(startupFailure: new InvalidOperationException("checkpoint опережает confirmed_flush."));
        harness.Coordinator.HandlePartitionsAssigned(Partitions(), CancellationToken.None);

        await TestAssert.AssertThrowsAsync<InvalidOperationException>(harness.Coordinator.Fatal);

        harness.Coordinator.IsLeading.ShouldBeFalse();
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
            Exception? startupFailure = null,
            Exception? failure = null)
        {
            Producer = new FakeProducer { OnCommit = onCommit };
            Registry = new CollectorRegistry();
            Metrics = new OutboxMetrics(Registry, slotName: "it_slot");
            Reader = new FakeSlotReader(messages, failure);
            Checks = new RecordingStartupChecks(startupFailure);

            var processorOptions = Options.Create(new ProcessorOptions { RetryDelaySeconds = 1 });
            var progress = new ReplicationProgress();
            var publisher = new OutboxPublisher(
                Producer,
                processorOptions,
                TimeProvider.System,
                Metrics,
                progress,
                new CheckpointStore(
                    Producer,
                    Options.Create(new KafkaOptions()),
                    Options.Create(new ReplicationOptions { SlotName = "it_slot" }),
                    TimeProvider.System,
                    NullLogger<CheckpointStore>.Instance),
                NullLogger<OutboxPublisher>.Instance);
            var services = new ServiceCollection();
            services.AddScoped<IOutboxStartupChecks>(_ => Checks);
            services.AddScoped(_ => new OutboxProcessor(
                Reader,
                publisher,
                processorOptions,
                Metrics,
                NullLogger<OutboxProcessor>.Instance));
            var provider = services.BuildServiceProvider();
            Coordinator = new LeadershipCoordinator(
                Producer,
                provider.GetRequiredService<IServiceScopeFactory>(),
                Metrics,
                NullLogger<LeadershipCoordinator>.Instance);
        }

        public FakeProducer Producer { get; }

        public CollectorRegistry Registry { get; }

        public OutboxMetrics Metrics { get; }

        public FakeSlotReader Reader { get; }

        public RecordingStartupChecks Checks { get; }

        public LeadershipCoordinator Coordinator { get; }
    }

    private sealed class RecordingStartupChecks(Exception? failure) : IOutboxStartupChecks
    {
        public int Invocations { get; private set; }

        public Task RunAllAsync(CancellationToken cancellationToken)
        {
            Invocations++;
            return failure is null ? Task.CompletedTask : Task.FromException(failure);
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
