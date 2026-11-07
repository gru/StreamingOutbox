namespace KafkaValidator.Contracts;

/// <summary>
/// Публичная сводка прогона без полного набора id (GET /api/validation/runs).
/// Дубликаты = records − uniqueIds (at-least-once, дубликаты допустимы).
/// </summary>
/// <param name="RunId">Идентификатор прогона.</param>
/// <param name="Records">Всего записей прогона (дубликаты включены).</param>
/// <param name="UniqueIds">Уникальных id.</param>
/// <param name="DuplicateRecords">Записей сверх уникальных id.</param>
/// <param name="FirstSeenUtc">Первая запись прогона в topic.</param>
/// <param name="LastSeenUtc">Последняя запись прогона.</param>
public sealed record RunSummary(
    Guid RunId,
    long Records,
    int UniqueIds,
    long DuplicateRecords,
    DateTimeOffset FirstSeenUtc,
    DateTimeOffset LastSeenUtc);
