namespace KafkaValidator.Contracts;

/// <summary>
/// Снимок состояния валидатора (GET /api/validation/state).
/// </summary>
/// <param name="State">running | stopped | failed | starting.</param>
/// <param name="StartedAtUtc">Момент старта процесса.</param>
/// <param name="StateChangedUtc">Момент последней смены состояния.</param>
/// <param name="Config">Config echo (брокеры/topic/группа/Writer).</param>
/// <param name="RecordsConsumed">Всего записей прочитано.</param>
/// <param name="RunsSeen">Прогонов замечено.</param>
/// <param name="SkippedNoRunId">Валидных записей без runId.</param>
/// <param name="ParseErrors">Записей с битым телом.</param>
/// <param name="LastRecordUtc">Момент последней записи; null — записей не было.</param>
/// <param name="LastError">Причина failed; null вне failed.</param>
public sealed record ValidatorStateSnapshot(
    string State,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset StateChangedUtc,
    ConfigEcho Config,
    long RecordsConsumed,
    int RunsSeen,
    long SkippedNoRunId,
    long ParseErrors,
    DateTimeOffset? LastRecordUtc,
    string? LastError);
