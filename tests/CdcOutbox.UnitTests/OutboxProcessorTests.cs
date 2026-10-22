using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
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
        var processor = new OutboxProcessor(metrics);
        using var cts = new CancellationTokenSource();

        await processor.StartAsync(cts.Token);

        var text = await ExportAsync(registry);
        text.ShouldContain("outbox_processor_is_active{partition=\"0\"} 1");
        text.ShouldContain("outbox_processor_last_iteration_timestamp_seconds{partition=\"0\"}");

        await processor.StopAsync(CancellationToken.None);

        text = await ExportAsync(registry);
        text.ShouldContain("outbox_processor_is_active{partition=\"0\"} 0");
    }

    private static async Task<string> ExportAsync(CollectorRegistry registry)
    {
        using var stream = new MemoryStream();
        await registry.CollectAndExportAsTextAsync(stream);
        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }
}
