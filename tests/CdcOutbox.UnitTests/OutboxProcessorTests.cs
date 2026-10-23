using System.Text;
using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
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

        var text = await ExportAsync(registry);
        text.ShouldContain("outbox_processor_is_active{partition=\"0\"} 1");
        text.ShouldContain("outbox_processor_last_iteration_timestamp_seconds{partition=\"0\"}");

        await processor.StopAsync(CancellationToken.None);

        text = await ExportAsync(registry);
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

        var text = await ExportAsync(registry);
        text.ShouldContain("outbox_processor_errors_total{partition=\"0\"} 2");
        text.ShouldContain("outbox_processor_is_active{partition=\"0\"} 1");
        reader.Attempts.ShouldBe(3);

        await processor.StopAsync(CancellationToken.None);

        text = await ExportAsync(registry);
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

        var text = await ExportAsync(registry);
        var errorSamples = text
            .Split('\n')
            .Where(line => line.StartsWith("outbox_processor_errors_total", StringComparison.Ordinal))
            .ToList();
        errorSamples.ShouldNotContain(line => !line.Contains("} 0"));
    }

    private static OutboxProcessor CreateProcessor(FakeSlotReader reader, OutboxMetrics metrics, int retryDelaySeconds = 5) =>
        new(
            reader,
            Options.Create(new ProcessorOptions { RetryDelaySeconds = retryDelaySeconds }),
            metrics,
            NullLogger<OutboxProcessor>.Instance);

    private static async Task<string> ExportAsync(CollectorRegistry registry)
    {
        using var stream = new MemoryStream();
        await registry.CollectAndExportAsTextAsync(stream);
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private sealed class FakeSlotReader(int failuresBeforeHold) : IOutboxSlotReader
    {
        public TaskCompletionSource Held { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Attempts { get; private set; }

        public async Task ReadAsync(ChannelWriter<OutboxMessage> output, CancellationToken cancellationToken)
        {
            Attempts++;
            if (Attempts <= failuresBeforeHold)
            {
                throw new InvalidOperationException("boom");
            }

            Held.SetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
    }
}
