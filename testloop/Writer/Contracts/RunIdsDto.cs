namespace Writer.Contracts;

/// <summary>
/// Ожидаемый набор id прогона для KafkaValidator (id — bigint таблицы outbox.messages).
/// </summary>
/// <param name="RunId">Идентификатор прогона.</param>
/// <param name="WrittenCount">Число записанных сообщений.</param>
/// <param name="AmbiguousCount">Число неоднозначно-завершённых сообщений.</param>
/// <param name="FailedCount">Число неудачных сообщений.</param>
/// <param name="WrittenIds">id записанных сообщений.</param>
/// <param name="AmbiguousIds">id неоднозначно-завершённых сообщений (не потеря).</param>
/// <param name="FailedIds">id неудачных сообщений (не доставлялись — в ожидаемый набор не входят).</param>
public sealed record RunIdsDto(
    Guid RunId,
    int WrittenCount,
    int AmbiguousCount,
    int FailedCount,
    List<long> WrittenIds,
    List<long> AmbiguousIds,
    List<long> FailedIds);
