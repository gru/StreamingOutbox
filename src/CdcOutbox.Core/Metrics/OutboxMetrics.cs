using Prometheus;

namespace CdcOutbox.Core.Metrics;

/// <summary>
/// Метрики outbox-процессора, экспортируемые через /metrics.
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
    private readonly Gauge _readMessages;
    private readonly Counter _errors;
    private readonly Counter _sentMessages;
    private readonly Histogram _publishLatency;
    private readonly Histogram _messageAge;
    private readonly Gauge _pendingEvents;

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

        _readMessages = factory.CreateGauge(
            "outbox_processor_read_messages_current",
            "Количество outbox-сообщений, декодированных из потока слота с момента запуска экземпляра (обнуляется при рестарте).",
            new GaugeConfiguration { LabelNames = new[] { "partition" } });

        _errors = factory.CreateCounter(
            "outbox_processor_errors_total",
            "Общее количество ошибок работы экземпляра outbox-процессора.",
            new CounterConfiguration { LabelNames = new[] { "partition" } });

        _sentMessages = factory.CreateCounter(
            "outbox_processor_sent_messages_total",
            "Количество outbox-сообщений, опубликованных в Kafka с подтверждением брокера, с момента запуска экземпляра.",
            new CounterConfiguration { LabelNames = new[] { "partition" } });

        _publishLatency = factory.CreateHistogram(
            "outbox_processor_publish_latency_seconds",
            "Время от отправки события в Kafka до получения подтверждения, с.",
            new HistogramConfiguration
            {
                Buckets = new[] { 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5 },
            });

        _messageAge = factory.CreateHistogram(
            "outbox_message_age_seconds",
            "Сквозная задержка от сохранения сообщения в outbox до подтверждения публикации, с (наблюдается в момент подтверждения).",
            new HistogramConfiguration
            {
                Buckets = new[] { 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300 },
            });

        _pendingEvents = factory.CreateGauge(
            "outbox_processor_pending_events_current",
            "Количество событий, отправленных в Kafka и ожидающих подтверждения.");
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
    /// Учесть прочитанное из слота outbox-сообщение.
    /// </summary>
    public void RegisterReadMessage()
    {
        _readMessages.WithLabels(_partition).Inc();
    }

    /// <summary>
    /// Зарегистрировать ошибку работы процессора.
    /// </summary>
    public void RegisterError()
    {
        _errors.WithLabels(_partition).Inc();
    }

    /// <summary>
    /// Учесть outbox-сообщение, подтверждённое брокером.
    /// </summary>
    public void RegisterSentMessage()
    {
        _sentMessages.WithLabels(_partition).Inc();
    }

    /// <summary>
    /// Зафиксировать задержку подтверждения публикации.
    /// </summary>
    /// <param name="seconds">Время от отправки в Kafka до подтверждения, с.</param>
    public void ObservePublishLatency(double seconds)
    {
        _publishLatency.Observe(seconds);
    }

    /// <summary>
    /// Зафиксировать сквозную задержку доставки сообщения.
    /// </summary>
    /// <param name="seconds">Время от сохранения в outbox до подтверждения, с.</param>
    public void ObserveMessageAge(double seconds)
    {
        _messageAge.Observe(seconds);
    }

    /// <summary>
    /// Изменить количество событий, ожидающих подтверждения Kafka.
    /// </summary>
    /// <param name="delta">Приращение: 1 при отправке, -1 при получении решения по репорту.</param>
    public void AdjustPendingEvents(int delta)
    {
        _pendingEvents.Inc(delta);
    }
}
