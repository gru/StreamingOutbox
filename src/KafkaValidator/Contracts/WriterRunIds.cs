namespace KafkaValidator.Contracts;

/// <summary>
/// Ответ Writer /api/generation/results/ids (RunIdsDto, camelCase).
/// </summary>
/// <param name="RunId">Идентификатор прогона.</param>
/// <param name="WrittenCount">Число записанных сообщений.</param>
/// <param name="AmbiguousCount">Число неоднозначно-завершённых сообщений.</param>
/// <param name="FailedCount">Число неудачных сообщений.</param>
/// <param name="WrittenIds">id записанных сообщений.</param>
/// <param name="AmbiguousIds">id неоднозначно-завершённых сообщений.</param>
/// <param name="FailedIds">id неудачных сообщений.</param>
public sealed record WriterRunIds(
    Guid RunId,
    int WrittenCount,
    int AmbiguousCount,
    int FailedCount,
    List<long> WrittenIds,
    List<long> AmbiguousIds,
    List<long> FailedIds);
