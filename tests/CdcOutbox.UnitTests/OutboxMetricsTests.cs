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

        var text = await MetricsExport.ExportAsync(registry);
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

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_is_active{partition=\"0\"} 0");
    }

    [Fact]
    public async Task RegisterError_Increments()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, "3");

        metrics.RegisterError();
        metrics.RegisterError();

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_errors_total{partition=\"3\"} 2");
    }

    [Fact]
    public async Task RegisterReadMessage_IncrementsGauge()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, "0");

        metrics.RegisterReadMessage();
        metrics.RegisterReadMessage();

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("# HELP outbox_processor_read_messages_current");
        text.ShouldContain("outbox_processor_read_messages_current{partition=\"0\"} 2");
    }

    [Fact]
    public async Task PublicationMetrics_ExposeSamples()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, "0");

        metrics.RegisterSentMessage();
        metrics.RegisterSentMessage();
        metrics.ObservePublishLatency(0.05);
        metrics.ObserveMessageAge(1.5);
        metrics.AdjustPendingEvents(1);
        metrics.AdjustPendingEvents(1);
        metrics.AdjustPendingEvents(-2);

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_sent_messages_total{partition=\"0\"} 2");
        text.ShouldContain("outbox_processor_publish_latency_seconds_count 1");
        text.ShouldContain("outbox_message_age_seconds_count 1");
        text.ShouldContain("outbox_processor_pending_events_current 0");
    }

    [Fact]
    public async Task CheckpointMetrics_ExposeSamples()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, "0");

        metrics.RegisterCheckpointSaved();
        metrics.SetCheckpointAgeSeconds(5);
        metrics.SetCheckpointLagBytes(-1);
        metrics.SetCheckpointLagBytes(123);
        metrics.RegisterCheckpointError();
        metrics.RegisterCheckpointError();

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("# HELP outbox_checkpoint_age_seconds");
        text.ShouldContain("outbox_checkpoint_age_seconds 5");
        text.ShouldContain("outbox_checkpoint_lag_bytes 123");
        text.ShouldContain("outbox_checkpoint_errors_total 2");
    }
}
