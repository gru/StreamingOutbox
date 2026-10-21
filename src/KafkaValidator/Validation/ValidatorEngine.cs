using KafkaValidator.Configuration;
using KafkaValidator.Contracts;
using KafkaValidator.Metrics;

namespace KafkaValidator.Validation;

/// <summary>
/// Состояние сервиса и снимок для GET /api/validation/state. Коды состояний:
/// starting → running (после старта KafkaFlow-шины; консюмер группы без
/// сохранения оффсетов уже читает topic с начала) → stopped/failed.
/// </summary>
public sealed class ValidatorEngine(KafkaValidatorMetrics metrics)
{
    private int _stateCode = KafkaValidatorMetrics.StateStarting;
    private string? _lastError;

    /// <summary>
    /// Момент старта процесса (UTC).
    /// </summary>
    public DateTimeOffset StartedAtUtc { get; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Момент последней смены состояния (UTC).
    /// </summary>
    public DateTimeOffset StateChangedUtc { get; private set; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Текущее состояние: running | stopped | failed | starting.
    /// </summary>
    public string State => _stateCode switch
    {
        KafkaValidatorMetrics.StateRunning => "running",
        KafkaValidatorMetrics.StateStopped => "stopped",
        KafkaValidatorMetrics.StateFailed => "failed",
        _ => "starting",
    };

    /// <summary>
    /// Перевести в running (вызывается lifecycle-сервисом после старта шины).
    /// </summary>
    public void MarkRunning()
    {
        SetState(KafkaValidatorMetrics.StateRunning);
    }

    /// <summary>
    /// Перевести в stopped (штатная остановка).
    /// </summary>
    public void MarkStopped()
    {
        SetState(KafkaValidatorMetrics.StateStopped);
    }

    /// <summary>
    /// Перевести в failed с причиной.
    /// </summary>
    public void MarkFailed(string error)
    {
        _lastError = error;
        SetState(KafkaValidatorMetrics.StateFailed);
    }

    private void SetState(int code)
    {
        _stateCode = code;
        StateChangedUtc = DateTimeOffset.UtcNow;
        metrics.ObserveState(code);
    }

    /// <summary>
    /// Снимок состояния для GET /api/validation/state: движок + счётчики
    /// реестра + config echo.
    /// </summary>
    public ValidatorStateSnapshot GetSnapshot(RunRegistry registry, KafkaValidatorOptions options) => new(
        State,
        StartedAtUtc,
        StateChangedUtc,
        new ConfigEcho(options.Kafka.Brokers, options.Kafka.Topic, options.Kafka.GroupId, options.Writer.BaseUrl),
        registry.RecordsTotal,
        registry.RunsSeen,
        registry.SkippedNoRunId,
        registry.ParseErrors,
        registry.LastRecordUtc,
        _lastError);
}
