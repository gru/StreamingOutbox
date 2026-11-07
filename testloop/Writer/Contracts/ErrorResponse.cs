namespace Writer.Contracts;

/// <summary>
/// Тело ошибки с одной причиной (400/404/409).
/// </summary>
/// <param name="Error">Текст ошибки.</param>
public sealed record ErrorResponse(string Error);
