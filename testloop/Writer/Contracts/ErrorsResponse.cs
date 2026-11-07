namespace Writer.Contracts;

/// <summary>
/// Тело 400 некорректного запроса: список ошибок (например, валидация профиля прогона).
/// </summary>
/// <param name="Errors">Ошибки запроса.</param>
public sealed record ErrorsResponse(IReadOnlyList<string> Errors);
