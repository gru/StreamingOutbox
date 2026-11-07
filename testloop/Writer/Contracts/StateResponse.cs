namespace Writer.Contracts;

/// <summary>
/// Тело ответа stop, когда итоги прогона ещё не готовы: текущее состояние движка.
/// </summary>
/// <param name="State">Текущее состояние (running/stopping/…).</param>
public sealed record StateResponse(string State);
