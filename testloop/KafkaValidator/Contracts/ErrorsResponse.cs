namespace KafkaValidator.Contracts;

/// <summary>
/// Тело 400 некорректного запроса: список ошибок.
/// </summary>
/// <param name="Errors">Ошибки запроса.</param>
public sealed record ErrorsResponse(IReadOnlyList<string> Errors);
