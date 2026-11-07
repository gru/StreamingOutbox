namespace KafkaValidator.Validation;

/// <summary>
/// Полный снимок прогона с множеством id — для сверки с ожидаемым набором
/// (VerdictComputer). Отдаётся только наружу из-под lock реестра.
/// </summary>
public sealed record RunDetail(
    Guid RunId,
    long Records,
    IReadOnlySet<long> UniqueIds,
    DateTimeOffset FirstSeenUtc,
    DateTimeOffset LastSeenUtc);
