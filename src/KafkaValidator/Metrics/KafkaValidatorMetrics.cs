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
    // Коды состояний — 0=starting 1=running 5=stopped 6=failed
    public const int StateStarting = 0;
    public const int StateRunning = 1;
    public const int StateStopped = 5;
    public const int StateFailed = 6;

    public Counter RecordsConsumed { get; }
    public Counter ParseErrors { get; }
    public Counter SkippedRecords { get; }
    public Counter Validations { get; }
    public Gauge State { get; }
    public Gauge RunsSeen { get; }

    public KafkaValidatorMetrics()
    {
        RecordsConsumed = Prometheus.Metrics.CreateCounter(
            "kafka_validator_records_consumed_total",
            "Записи topic, прочитанные валидатором (включая перемотку после рестарта)");
        ParseErrors = Prometheus.Metrics.CreateCounter(
            "kafka_validator_parse_errors_total",
            "Записи с битым телом: reason=malformed", "reason");
        SkippedRecords = Prometheus.Metrics.CreateCounter(
            "kafka_validator_skipped_records_total",
            "Валидные записи без body.runId (ручные INSERT и т.п.): reason=no_run_id", "reason");
        Validations = Prometheus.Metrics.CreateCounter(
            "kafka_validator_validations_total",
            "Выполненные валидации прогонов: verdict=pass|fail", "verdict");
        State = Prometheus.Metrics.CreateGauge(
            "kafka_validator_state",
            "Состояние сервиса: 0=starting 1=running 5=stopped 6=failed");
        RunsSeen = Prometheus.Metrics.CreateGauge(
            "kafka_validator_runs_seen",
            "Прогонов (runId), замеченных в topic с момента старта процесса");
    }

    /// <summary>Надёжный liveness-показатель: счётчик без записей не экспортируется вовсе, gauge — всегда.</summary>
    public void ObserveState(int code) => State.Set(code);

    /// <summary>Число прогонов в реестре (реестр — источник правды).</summary>
    public void ObserveRunsSeen(int count) => RunsSeen.Set(count);

    public void AddParseError(string reason) =>
        ParseErrors.WithLabels(reason).Inc();

    public void AddSkipped(string reason) =>
        SkippedRecords.WithLabels(reason).Inc();

    public void AddValidation(bool pass) =>
        Validations.WithLabels(pass ? "pass" : "fail").Inc();
}
