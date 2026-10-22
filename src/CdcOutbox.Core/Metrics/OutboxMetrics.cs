using Prometheus;

namespace CdcOutbox.Core.Metrics;

/// <summary>
/// Метрики outbox-процессора (metrics.md), экспортируемые через /metrics.
/// </summary>
public sealed class OutboxMetrics
{
    /// <summary>
    /// Label-значение partition, когда процессор обслуживает единственную партицию outbox.
    /// </summary>
    public const string DefaultPartition = "0";

    private readonly string _partition;
    private readonly Gauge _isActive;
    private readonly Gauge _lastIterationTimestamp;
    private readonly Counter _errors;

    /// <summary>
    /// Метрики на переданном реестре (в сервисе — DefaultRegistry, экспортируемый /metrics).
    /// </summary>
    /// <param name="registry">Реестр Prometheus.</param>
    /// <param name="partition">Значение label partition.</param>
    public OutboxMetrics(CollectorRegistry registry, string partition = DefaultPartition)
    {
        _partition = partition;
        var factory = Prometheus.Metrics.WithCustomRegistry(registry);

        _isActive = factory.CreateGauge(
            "outbox_processor_is_active",
            "Активность экземпляра outbox-процессора: 1 — работает, 0 — нет.",
            new GaugeConfiguration { LabelNames = new[] { "partition" } });

        _lastIterationTimestamp = factory.CreateGauge(
            "outbox_processor_last_iteration_timestamp_seconds",
            "Unix timestamp (с) запуска последней итерации цикла обработки.",
            new GaugeConfiguration { LabelNames = new[] { "partition" } });

        _errors = factory.CreateCounter(
            "outbox_processor_errors_total",
            "Общее количество ошибок работы экземпляра outbox-процессора.",
            new CounterConfiguration { LabelNames = new[] { "partition" } });
    }

    /// <summary>
    /// Отметить активность процессора (1) или остановку (0).
    /// </summary>
    /// <param name="active">Признак активности.</param>
    public void SetActive(bool active)
    {
        _isActive.WithLabels(_partition).Set(active ? 1 : 0);
    }

    /// <summary>
    /// Зафиксировать запуск очередной итерации цикла обработки.
    /// </summary>
    public void TouchIteration()
    {
        _lastIterationTimestamp.WithLabels(_partition).SetToCurrentTimeUtc();
    }

    /// <summary>
    /// Зарегистрировать ошибку работы процессора.
    /// </summary>
    public void RegisterError()
    {
        _errors.WithLabels(_partition).Inc();
    }
}
