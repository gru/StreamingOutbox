using Prometheus;

namespace KafkaValidator.Metrics;

/// <summary>
/// Метрики KafkaValidator — prometheus-net (pull /metrics, dependencies.md §2).
/// Кардинальность фиксированная: runId и message id как label ЗАПРЕЩЕНЫ —
/// счётчики глобальные, детализация по прогонам доступна через HTTP API
/// (/api/validation/runs, /api/validation/validate) из фактических данных,
/// а не через Prometheus (scrape не гарантирует сохранение кратковременных
/// событий).
/// ВНИМАНИЕ: Prometheus.Metrics квалифицируется полностью — внутри namespace
/// KafkaValidator.Metrics имя Metrics резолвится в сам namespace.
/// </summary>
public sealed class KafkaValidatorMetrics
{
    /// <summary>
    /// Код состояния starting (запуск процесса/шины).
    /// </summary>
    public const int StateStarting = 0;

    /// <summary>
    /// Код состояния running (KafkaFlow-шина запущена).
    /// </summary>
    public const int StateRunning = 1;

    /// <summary>
    /// Код состояния stopped (штатная остановка).
    /// </summary>
    public const int StateStopped = 5;

    /// <summary>
    /// Код состояния failed.
    /// </summary>
    public const int StateFailed = 6;

    /// <summary>
    /// Записи topic, прочитанные валидатором (kafka_validator_records_consumed_total).
    /// </summary>
    public Counter RecordsConsumed { get; } = Prometheus.Metrics.CreateCounter(
        "kafka_validator_records_consumed_total",
        "Записи topic, прочитанные валидатором (включая перемотку после рестарта)");

    /// <summary>
    /// Записи с битым телом по reason (kafka_validator_parse_errors_total).
    /// </summary>
    public Counter ParseErrors { get; } = Prometheus.Metrics.CreateCounter(
        "kafka_validator_parse_errors_total",
        "Записи с битым телом: reason=malformed", "reason");

    /// <summary>
    /// Валидные записи без body.runId по reason (kafka_validator_skipped_records_total).
    /// </summary>
    public Counter SkippedRecords { get; } = Prometheus.Metrics.CreateCounter(
        "kafka_validator_skipped_records_total",
        "Валидные записи без body.runId (ручные INSERT и т.п.): reason=no_run_id", "reason");

    /// <summary>
    /// Выполненные валидации по verdict (kafka_validator_validations_total).
    /// </summary>
    public Counter Validations { get; } = Prometheus.Metrics.CreateCounter(
        "kafka_validator_validations_total",
        "Выполненные валидации прогонов: verdict=pass|fail", "verdict");

    /// <summary>
    /// Состояние сервиса: 0=starting 1=running 5=stopped 6=failed
    /// (kafka_validator_state).
    /// </summary>
    public Gauge State { get; } = Prometheus.Metrics.CreateGauge(
        "kafka_validator_state",
        "Состояние сервиса: 0=starting 1=running 5=stopped 6=failed");

    /// <summary>
    /// Прогонов (runId), замеченных в topic с момента старта процесса
    /// (kafka_validator_runs_seen).
    /// </summary>
    public Gauge RunsSeen { get; } = Prometheus.Metrics.CreateGauge(
        "kafka_validator_runs_seen",
        "Прогонов (runId), замеченных в topic с момента старта процесса");

    /// <summary>
    /// Надёжный liveness-показатель: счётчик без записей не экспортируется вовсе, gauge — всегда.
    /// </summary>
    public void ObserveState(int code) => State.Set(code);

    /// <summary>
    /// Число прогонов в реестре (реестр — источник правды).
    /// </summary>
    public void ObserveRunsSeen(int count) => RunsSeen.Set(count);

    /// <summary>
    /// Инкремент parse_errors по reason.
    /// </summary>
    public void AddParseError(string reason) =>
        ParseErrors.WithLabels(reason).Inc();

    /// <summary>
    /// Инкремент skipped_records по reason.
    /// </summary>
    public void AddSkipped(string reason) =>
        SkippedRecords.WithLabels(reason).Inc();

    /// <summary>
    /// Инкремент validations по verdict (pass|fail).
    /// </summary>
    public void AddValidation(bool pass) =>
        Validations.WithLabels(pass ? "pass" : "fail").Inc();
}
