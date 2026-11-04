using CdcOutbox.Core.Metrics;
using Prometheus;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class OutboxMetricsTests
{
    [Fact]
    public async Task Metrics_ExposeNamesAndSlotLabel()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");

        metrics.SetActive(true);
        metrics.TouchIteration();
        metrics.RegisterError();

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("# HELP outbox_processor_is_active");
        text.ShouldContain("outbox_processor_is_active{slot=\"it_slot\"} 1");
        text.ShouldContain("outbox_processor_last_iteration_timestamp_seconds{slot=\"it_slot\"}");
        text.ShouldContain("outbox_processor_errors_total{slot=\"it_slot\"} 1");
    }

    [Fact]
    public async Task SetActive_False_ExportsZero()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");

        metrics.SetActive(true);
        metrics.SetActive(false);

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_is_active{slot=\"it_slot\"} 0");
    }

    [Fact]
    public async Task RegisterError_Increments()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");

        metrics.RegisterError();
        metrics.RegisterError();

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_errors_total{slot=\"it_slot\"} 2");
    }

    [Fact]
    public async Task RegisterReadMessage_IncrementsGauge()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");

        metrics.RegisterReadMessage();
        metrics.RegisterReadMessage();

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("# HELP outbox_processor_read_messages_current");
        text.ShouldContain("outbox_processor_read_messages_current{slot=\"it_slot\"} 2");
    }

    [Fact]
    public async Task PublicationMetrics_ExposeSamples()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");

        metrics.RegisterSentMessage();
        metrics.RegisterSentMessage();
        metrics.ObservePublishLatency(0.05);
        metrics.ObserveMessageAge(1.5);
        metrics.AdjustPendingEvents(1);
        metrics.AdjustPendingEvents(1);
        metrics.AdjustPendingEvents(-2);

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_sent_messages_total{slot=\"it_slot\"} 2");
        text.ShouldContain("outbox_processor_publish_latency_seconds_count{slot=\"it_slot\"} 1");
        text.ShouldContain("outbox_message_age_seconds_count{slot=\"it_slot\"} 1");
        text.ShouldContain("outbox_processor_pending_events_current{slot=\"it_slot\"} 0");
    }

    [Fact]
    public async Task CheckpointMetrics_ExposeSamples()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");

        metrics.RegisterCheckpointSaved();
        metrics.SetCheckpointAgeSeconds(5);
        metrics.SetCheckpointLagBytes(-1);
        metrics.SetCheckpointLagBytes(123);
        metrics.RegisterCheckpointError();
        metrics.RegisterCheckpointError();

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("# HELP outbox_checkpoint_age_seconds");
        text.ShouldContain("outbox_checkpoint_age_seconds{slot=\"it_slot\"} 5");
        text.ShouldContain("outbox_checkpoint_lag_bytes{slot=\"it_slot\"} 123");
        text.ShouldContain("outbox_checkpoint_errors_total{slot=\"it_slot\"} 2");
    }

    [Fact]
    public async Task RegisterSlotReconnect_Increments()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");

        metrics.RegisterSlotReconnect();
        metrics.RegisterSlotReconnect();
        metrics.RegisterSlotReconnect();

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("# HELP outbox_slot_reconnects_total");
        text.ShouldContain("outbox_slot_reconnects_total{slot=\"it_slot\"} 3");
    }

    [Fact]
    public async Task SetRecoveryMode_ExportsSlotLabel()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");

        metrics.SetRecoveryMode(true);

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("# HELP outbox_recovery_mode");
        text.ShouldContain("outbox_recovery_mode{slot=\"it_slot\"} 1");
    }

    [Fact]
    public async Task SetRecoveryMode_False_ExportsZero()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");

        metrics.SetRecoveryMode(false);

        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_recovery_mode{slot=\"it_slot\"} 0");
    }
}
