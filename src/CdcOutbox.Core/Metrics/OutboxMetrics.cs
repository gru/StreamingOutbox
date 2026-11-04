using Prometheus;

namespace CdcOutbox.Core.Metrics;

/// <summary>
/// Метрики outbox-процессора, экспортируемые через /metrics.
/// </summary>
public sealed class OutboxMetrics
{
    private readonly string _slotName;
    private readonly Gauge _isActive;
    private readonly Gauge _lastIterationTimestamp;
    private readonly Gauge _readMessages;
    private readonly Counter _errors;
    private readonly Counter _sentMessages;
    private readonly Histogram _publishLatency;
    private readonly Histogram _messageAge;
    private readonly Gauge _pendingEvents;
    private readonly Gauge _checkpointAge;
    private readonly Gauge _checkpointLag;
    private readonly Counter _checkpointErrors;
    private readonly Counter _slotReconnects;
    private readonly Gauge _recoveryMode;

    /// <summary>
    /// Метрики на переданном реестре (в сервисе — DefaultRegistry, экспортируемый /metrics).
    /// </summary>
    /// <param name="registry">Реестр Prometheus.</param>
    /// <param name="slotName">Имя replication slot — значение label slot (из <c>ReplicationOptions.SlotName</c>, без дефолта: имя слота — конфигурация, а не константа).</param>
    public OutboxMetrics(CollectorRegistry registry, string slotName)
    {
        _slotName = slotName;
        var factory = Prometheus.Metrics.WithCustomRegistry(registry);

        _isActive = factory.CreateGauge(
            "outbox_processor_is_active",
            "Активность экземпляра outbox-процессора: 1 — работает, 0 — нет.",
            new GaugeConfiguration { LabelNames = new[] { "slot" } });

        _lastIterationTimestamp = factory.CreateGauge(
            "outbox_processor_last_iteration_timestamp_seconds",
            "Unix timestamp (с) запуска последней итерации цикла обработки.",
            new GaugeConfiguration { LabelNames = new[] { "slot" } });

        _readMessages = factory.CreateGauge(
            "outbox_processor_read_messages_current",
            "Количество outbox-сообщений, декодированных из потока слота с момента запуска экземпляра (обнуляется при рестарте).",
            new GaugeConfiguration { LabelNames = new[] { "slot" } });

        _errors = factory.CreateCounter(
            "outbox_processor_errors_total",
            "Общее количество ошибок работы экземпляра outbox-процессора.",
            new CounterConfiguration { LabelNames = new[] { "slot" } });

        _sentMessages = factory.CreateCounter(
            "outbox_processor_sent_messages_total",
            "Количество outbox-сообщений, опубликованных в Kafka с подтверждением брокера, с момента запуска экземпляра.",
            new CounterConfiguration { LabelNames = new[] { "slot" } });

        _publishLatency = factory.CreateHistogram(
            "outbox_processor_publish_latency_seconds",
            "Время от отправки события в Kafka до получения подтверждения, с.",
            new HistogramConfiguration
            {
                LabelNames = new[] { "slot" },
                Buckets = new[] { 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5 },
            });

        _messageAge = factory.CreateHistogram(
            "outbox_message_age_seconds",
            "Сквозная задержка от сохранения сообщения в outbox до подтверждения публикации, с (наблюдается в момент подтверждения).",
            new HistogramConfiguration
            {
                LabelNames = new[] { "slot" },
                Buckets = new[] { 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300 },
            });

        _pendingEvents = factory.CreateGauge(
            "outbox_processor_pending_events_current",
            "Количество событий, отправленных в Kafka и ожидающих подтверждения.",
            new GaugeConfiguration { LabelNames = new[] { "slot" } });

        _checkpointAge = factory.CreateGauge(
            "outbox_checkpoint_age_seconds",
            "Время с последнего подтверждённо сохранённого checkpoint, с.",
            new GaugeConfiguration { LabelNames = new[] { "slot" } });

        _checkpointLag = factory.CreateGauge(
            "outbox_checkpoint_lag_bytes",
            "Разница между прочитанной позицией WAL и позицией подтверждённо сохранённого checkpoint, байт.",
            new GaugeConfiguration { LabelNames = new[] { "slot" } });

        _checkpointErrors = factory.CreateCounter(
            "outbox_checkpoint_errors_total",
            "Общее количество ошибок сохранения checkpoint.",
            new CounterConfiguration { LabelNames = new[] { "slot" } });

        _slotReconnects = factory.CreateCounter(
            "outbox_slot_reconnects_total",
            "Количество переподключений к replication slot (разрывы walsender: failover/switchover PostgreSQL, сетевые сбои, backpressure-таймауты).",
            new CounterConfiguration { LabelNames = new[] { "slot" } });

        _recoveryMode = factory.CreateGauge(
            "outbox_recovery_mode",
            "Recovery Mode: 1 — процесс запущен с аварийным восстановлением (переписывание checkpoint по позиции слота), 0 — обычный режим.",
            new GaugeConfiguration { LabelNames = new[] { "slot" } });
    }

    /// <summary>
    /// Отметить активность процессора (1) или остановку (0).
    /// </summary>
    /// <param name="active">Признак активности.</param>
    public void SetActive(bool active)
    {
        _isActive.WithLabels(_slotName).Set(active ? 1 : 0);
    }

    /// <summary>
    /// Зафиксировать запуск очередной итерации цикла обработки.
    /// </summary>
    public void TouchIteration()
    {
        _lastIterationTimestamp.WithLabels(_slotName).SetToCurrentTimeUtc();
    }

    /// <summary>
    /// Учесть прочитанное из слота outbox-сообщение.
    /// </summary>
    public void RegisterReadMessage()
    {
        _readMessages.WithLabels(_slotName).Inc();
    }

    /// <summary>
    /// Зарегистрировать ошибку работы процессора.
    /// </summary>
    public void RegisterError()
    {
        _errors.WithLabels(_slotName).Inc();
    }

    /// <summary>
    /// Учесть outbox-сообщение, подтверждённое брокером.
    /// </summary>
    public void RegisterSentMessage()
    {
        _sentMessages.WithLabels(_slotName).Inc();
    }

    /// <summary>
    /// Зафиксировать задержку подтверждения публикации.
    /// </summary>
    /// <param name="seconds">Время от отправки в Kafka до подтверждения, с.</param>
    public void ObservePublishLatency(double seconds)
    {
        _publishLatency.WithLabels(_slotName).Observe(seconds);
    }

    /// <summary>
    /// Зафиксировать сквозную задержку доставки сообщения.
    /// </summary>
    /// <param name="seconds">Время от сохранения в outbox до подтверждения, с.</param>
    public void ObserveMessageAge(double seconds)
    {
        _messageAge.WithLabels(_slotName).Observe(seconds);
    }

    /// <summary>
    /// Изменить количество событий, ожидающих подтверждения Kafka.
    /// </summary>
    /// <param name="delta">Приращение: 1 при отправке, -1 при получении решения по репорту.</param>
    public void AdjustPendingEvents(int delta)
    {
        _pendingEvents.WithLabels(_slotName).Inc(delta);
    }

    /// <summary>
    /// Зафиксировать подтверждённо сохранённый checkpoint (возраст сбрасывается в ноль).
    /// </summary>
    public void RegisterCheckpointSaved()
    {
        _checkpointAge.WithLabels(_slotName).Set(0);
    }

    /// <summary>
    /// Установить текущий возраст последнего сохранённого checkpoint, с.
    /// </summary>
    /// <param name="seconds">Время с последнего сохранённого checkpoint.</param>
    public void SetCheckpointAgeSeconds(double seconds)
    {
        _checkpointAge.WithLabels(_slotName).Set(seconds < 0 ? 0 : seconds);
    }

    /// <summary>
    /// Установить отставание checkpoint от прочитанной позиции WAL, байт.
    /// </summary>
    /// <param name="bytes">Разница позиций.</param>
    public void SetCheckpointLagBytes(long bytes)
    {
        _checkpointLag.WithLabels(_slotName).Set(bytes > 0 ? bytes : 0);
    }

    /// <summary>
    /// Зарегистрировать ошибку сохранения checkpoint.
    /// </summary>
    public void RegisterCheckpointError()
    {
        _checkpointErrors.WithLabels(_slotName).Inc();
    }

    /// <summary>
    /// Зарегистрировать переподключение к replication slot (каждая сессия чтения, кроме первой).
    /// </summary>
    public void RegisterSlotReconnect()
    {
        _slotReconnects.WithLabels(_slotName).Inc();
    }

    /// <summary>
    /// Отметить, что процесс запущен в Recovery Mode (1) или в обычном режиме (0).
    /// </summary>
    /// <param name="enabled">Признак recovery-режима.</param>
    public void SetRecoveryMode(bool enabled)
    {
        _recoveryMode.WithLabels(_slotName).Set(enabled ? 1 : 0);
    }
}
