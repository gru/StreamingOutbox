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

namespace CdcOutbox.UnitTests;

public sealed class OutboxProcessorTests
{
    [Fact]
    public async Task Start_MarksActive_Stop_Clears()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry);
        var reader = new FakeSlotReader(failuresBeforeHold: 0);
        var processor = CreateProcessor(reader, metrics);
        using var cts = new CancellationTokenSource();

        await processor.StartAsync(cts.Token);
        await reader.Held.Task;

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_is_active{partition=\"0\"} 1");
        text.ShouldContain("outbox_processor_last_iteration_timestamp_seconds{partition=\"0\"}");

        await processor.StopAsync(CancellationToken.None);

        text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_is_active{partition=\"0\"} 0");
    }

    [Fact]
    public async Task ReaderFailures_RegisterErrorsAndKeepRetrying()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry);
        var reader = new FakeSlotReader(failuresBeforeHold: 2);
        var processor = CreateProcessor(reader, metrics, retryDelaySeconds: 1);
        using var cts = new CancellationTokenSource();

        await processor.StartAsync(cts.Token);
        await reader.Held.Task;

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_errors_total{partition=\"0\"} 2");
        text.ShouldContain("outbox_processor_is_active{partition=\"0\"} 1");
        reader.Attempts.ShouldBe(3);

        await processor.StopAsync(CancellationToken.None);

        text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_is_active{partition=\"0\"} 0");
    }

    [Fact]
    public async Task Cancellation_DoesNotRegisterError()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry);
        var reader = new FakeSlotReader(failuresBeforeHold: 0);
        var processor = CreateProcessor(reader, metrics);
        using var cts = new CancellationTokenSource();

        await processor.StartAsync(cts.Token);
        await reader.Held.Task;
        await processor.StopAsync(CancellationToken.None);

        var text = await MetricsExport.ExportAsync(registry);
        var errorSamples = text
            .Split('\n')
            .Where(line => line.StartsWith("outbox_processor_errors_total", StringComparison.Ordinal))
            .ToList();
        errorSamples.ShouldNotContain(line => !line.Contains("} 0"));
    }

    [Fact]
    public async Task ReaderMessages_ArePublishedToKafka()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry);
        var producer = new FakeProducer();
        var reader = new FakeSlotReader(failuresBeforeHold: 0, new[] { CreateMessage(1), CreateMessage(2) });
        var processor = CreateProcessor(reader, metrics, producer);
        using var cts = new CancellationTokenSource();

        await processor.StartAsync(cts.Token);
        await reader.Held.Task;
        await processor.StopAsync(CancellationToken.None);

        producer.Produced.Count.ShouldBe(2);
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_sent_messages_total{partition=\"0\"} 2");
        text.ShouldContain("outbox_processor_pending_events_current 0");
    }

    private static OutboxProcessor CreateProcessor(
        FakeSlotReader reader,
        OutboxMetrics metrics,
        FakeProducer? producer = null,
        int retryDelaySeconds = 5)
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
            NullLogger<OutboxProcessor>.Instance);
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

    private sealed class FakeSlotReader(int failuresBeforeHold, OutboxMessage[]? messages = null) : IOutboxSlotReader
    {
        public TaskCompletionSource Held { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Attempts { get; private set; }

        public async Task ReadAsync(ChannelWriter<OutboxMessage> output, CancellationToken cancellationToken)
        {
            Attempts++;
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
