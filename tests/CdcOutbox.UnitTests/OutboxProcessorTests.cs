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
using Npgsql;
using Prometheus;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class OutboxProcessorTests
{
    [Fact]
    public async Task Run_TouchesIteration_CompletesOnCancel()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var reader = new FakeSlotReader(failuresBeforeHold: 0);
        var processor = CreateProcessor(reader, metrics);
        using var cts = new CancellationTokenSource();

        var running = processor.RunAsync(cts.Token);
        await reader.Held.Task;

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_last_iteration_timestamp_seconds{slot=\"it_slot\"}");

        await cts.CancelAsync();
        await running;

        running.IsCompletedSuccessfully.ShouldBeTrue();
    }

    [Fact]
    public async Task ReaderFailures_RegisterErrorsAndKeepRetrying()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var reader = new FakeSlotReader(failuresBeforeHold: 2);
        var processor = CreateProcessor(reader, metrics, retryDelaySeconds: 1);
        using var cts = new CancellationTokenSource();

        var running = processor.RunAsync(cts.Token);
        await reader.Held.Task;

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_errors_total{slot=\"it_slot\"} 2");
        text.ShouldContain("outbox_slot_reconnects_total{slot=\"it_slot\"} 2");
        reader.Attempts.ShouldBe(3);

        await cts.CancelAsync();
        await running;
    }

    [Fact]
    public async Task TerminalSlotInvalidation_StopsProcessorAfterSingleAttempt()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var logger = new RecordingLogger<OutboxProcessor>();
        var reader = new FakeSlotReader(
            failuresBeforeHold: 0,
            failure: new PostgresException(
                "logical replication slot \"outbox_slot\" has been invalidated due to WAL removal",
                "ERROR", "ERROR", PostgresErrorCodes.ObjectNotInPrerequisiteState));
        var processor = CreateProcessor(reader, metrics, logger: logger);
        using var cts = new CancellationTokenSource();

        var running = processor.RunAsync(cts.Token);
        var stopped = await TestWait.WaitForAsync(
            () => logger.Entries.Any(entry => entry.Level == LogLevel.Critical),
            TimeSpan.FromSeconds(10));

        stopped.ShouldBeTrue();
        reader.Attempts.ShouldBe(1);
        var failure = await TestAssert.AssertThrowsAsync<InvalidOperationException>(running);
        failure.ShouldNotBeNull();
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_errors_total{slot=\"it_slot\"} 1");
    }

    [Fact]
    public async Task Cancellation_DoesNotRegisterError()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var reader = new FakeSlotReader(failuresBeforeHold: 0);
        var processor = CreateProcessor(reader, metrics);
        using var cts = new CancellationTokenSource();

        var running = processor.RunAsync(cts.Token);
        await reader.Held.Task;
        await cts.CancelAsync();
        await running;

        var text = await MetricsExport.ExportAsync(registry);
        var errorSamples = text
            .Split('\n')
            .Where(line => line.StartsWith("outbox_processor_errors_total", StringComparison.Ordinal))
            .ToList();
        errorSamples.ShouldNotContain(line => !line.Contains("} 0"));
    }

    [Fact]
    public async Task ReaderMessages_ArePublishedToKafkaInTransaction()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var producer = new FakeProducer();
        var reader = new FakeSlotReader(failuresBeforeHold: 0, new[] { CreateMessage(1), CreateMessage(2) });
        var processor = CreateProcessor(reader, metrics, producer);
        using var cts = new CancellationTokenSource();

        var running = processor.RunAsync(cts.Token);
        await reader.Held.Task;
        await cts.CancelAsync();
        await running;

        producer.Produced.Count.ShouldBe(2);
        producer.TransactionCalls.ShouldContain("begin");
        producer.TransactionCalls.ShouldContain("commit");
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_sent_messages_total{slot=\"it_slot\"} 2");
        text.ShouldContain("outbox_processor_pending_events_current{slot=\"it_slot\"} 0");
    }

    private static OutboxProcessor CreateProcessor(
        FakeSlotReader reader,
        OutboxMetrics metrics,
        FakeProducer? producer = null,
        int retryDelaySeconds = 5,
        ILogger<OutboxProcessor>? logger = null)
    {
        var processorOptions = new ProcessorOptions { RetryDelaySeconds = retryDelaySeconds };
        var effectiveProducer = producer ?? new FakeProducer();
        return new OutboxProcessor(
            reader,
            new OutboxPublisher(
                effectiveProducer,
                Options.Create(processorOptions),
                TimeProvider.System,
                metrics,
                new ReplicationProgress(),
                new CheckpointStore(
                    effectiveProducer,
                    Options.Create(new KafkaOptions()),
                    Options.Create(new ReplicationOptions { SlotName = "outbox_unit_slot" }),
                    TimeProvider.System,
                    NullLogger<CheckpointStore>.Instance),
                NullLogger<OutboxPublisher>.Instance),
            Options.Create(processorOptions),
            metrics,
            logger ?? NullLogger<OutboxProcessor>.Instance);
    }

    private static OutboxMessage CreateMessage(long id) =>
        new()
        {
            Id = id,
            Topic = "orders.events",
            Key = new byte[] { (byte)id },
            Body = new byte[] { (byte)id },
            Headers = "{}",
            CreatedAt = DateTime.UtcNow,
        };

    private sealed class FakeSlotReader(int failuresBeforeHold, OutboxMessage[]? messages = null, Exception? failure = null)
        : IOutboxSlotReader
    {
        public TaskCompletionSource Held { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Attempts { get; private set; }

        public async Task ReadAsync(ChannelWriter<OutboxMessage> output, CancellationToken cancellationToken)
        {
            Attempts++;
            if (failure is not null)
            {
                throw failure;
            }

            if (Attempts <= failuresBeforeHold)
            {
                throw new InvalidOperationException("Имитация сбоя чтения слота.");
            }

            foreach (var message in messages ?? Array.Empty<OutboxMessage>())
            {
                await output.WriteAsync(message, cancellationToken);
            }

            Held.SetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
    }
}
