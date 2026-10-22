using System.Text;
using CdcOutbox.Core.Metrics;
using Prometheus;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class OutboxMetricsTests
{
    [Fact]
    public async Task Metrics_ExposeNamesAndPartitionLabel()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, "0");

        metrics.SetActive(true);
        metrics.TouchIteration();
        metrics.RegisterError();

        var text = await ExportAsync(registry);
        text.ShouldContain("# HELP outbox_processor_is_active");
        text.ShouldContain("outbox_processor_is_active{partition=\"0\"} 1");
        text.ShouldContain("outbox_processor_last_iteration_timestamp_seconds{partition=\"0\"}");
        text.ShouldContain("outbox_processor_errors_total{partition=\"0\"} 1");
    }

    [Fact]
    public async Task SetActive_False_ExportsZero()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, "0");

        metrics.SetActive(true);
        metrics.SetActive(false);

        var text = await ExportAsync(registry);
        text.ShouldContain("outbox_processor_is_active{partition=\"0\"} 0");
    }

    [Fact]
    public async Task RegisterError_Increments()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, "3");

        metrics.RegisterError();
        metrics.RegisterError();

        var text = await ExportAsync(registry);
        text.ShouldContain("outbox_processor_errors_total{partition=\"3\"} 2");
    }

    private static async Task<string> ExportAsync(CollectorRegistry registry)
    {
        using var stream = new MemoryStream();
        await registry.CollectAndExportAsTextAsync(stream);
        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
