namespace KafkaValidator.Contracts;

/// <summary>
/// Тело 404 незаметенного в topic прогона.
/// </summary>
/// <param name="RunId">Запрошенный идентификатор прогона.</param>
public sealed record RunNotFoundResponse(Guid RunId);
