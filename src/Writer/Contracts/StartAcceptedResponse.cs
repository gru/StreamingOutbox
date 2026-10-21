using Writer.Generation;

namespace Writer.Contracts;

/// <summary>
/// Тело 202 запуска прогона (Location: /api/generation/state).
/// </summary>
/// <param name="RunId">Идентификатор запущенного прогона.</param>
/// <param name="Profile">Разрешённый профиль прогона.</param>
public sealed record StartAcceptedResponse(Guid RunId, LoadProfile Profile);
