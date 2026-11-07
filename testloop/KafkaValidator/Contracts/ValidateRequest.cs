namespace KafkaValidator.Contracts;

/// <summary>
/// Тело POST /api/validation/validate (camelCase из JSON).
/// </summary>
public sealed class ValidateRequest
{
    /// <summary>
    /// Идентификатор прогона (body.runId записей Writer).
    /// </summary>
    public Guid? RunId { get; set; }

    /// <summary>
    /// true — ожидаемый набор взять у Writer (/api/generation/results/ids);
    /// действует только для ПОСЛЕДНЕГО завершённого прогона (при несовпадении
    /// runId — 409). Для старых прогонов передавайте сохранённые массивы явно.
    /// </summary>
    public bool FetchFromWriter { get; set; }

    /// <summary>
    /// Явный ожидаемый набор written id (альтернатива fetchFromWriter).
    /// </summary>
    public List<long>? WrittenIds { get; set; }

    /// <summary>
    /// Явный набор ambiguous id (неопределённые коммиты Writer).
    /// </summary>
    public List<long>? AmbiguousIds { get; set; }

    /// <summary>
    /// Явный набор failed id (не доставлялись).
    /// </summary>
    public List<long>? FailedIds { get; set; }
}
