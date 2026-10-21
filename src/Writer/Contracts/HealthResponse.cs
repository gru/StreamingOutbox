namespace Writer.Contracts;

/// <summary>
/// Тело /health.
/// </summary>
/// <param name="Status">Всегда "Healthy" — процесс жив.</param>
public sealed record HealthResponse(string Status);
