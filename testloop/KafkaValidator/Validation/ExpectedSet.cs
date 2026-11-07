namespace KafkaValidator.Validation;

/// <summary>
/// Ожидаемый набор id прогона: источник — Writer (GET /api/generation/results/ids)
/// или явные массивы в запросе (сохранённый на время теста набор).
/// id — bigint таблицы outbox.messages.
/// </summary>
/// <param name="RunId">Идентификатор прогона.</param>
/// <param name="WrittenIds">id, зафиксированные commit'ом Writer.</param>
/// <param name="AmbiguousIds">id с неопределённым исходом commit (не потеря).</param>
/// <param name="FailedIds">id незавершённой записи (в Kafka быть не могут).</param>
/// <param name="Source">Источник набора: SourceWriter | SourceExplicit.</param>
public sealed record ExpectedSet(
    Guid RunId,
    IReadOnlySet<long> WrittenIds,
    IReadOnlySet<long> AmbiguousIds,
    IReadOnlySet<long> FailedIds,
    string Source)
{
    /// <summary>
    /// Набор взят у Writer (последний завершённый прогон).
    /// </summary>
    public const string SourceWriter = "writer";

    /// <summary>
    /// Набор передан явными массивами в запросе.
    /// </summary>
    public const string SourceExplicit = "explicit";
}
