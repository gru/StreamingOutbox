namespace KafkaValidator.Contracts;

/// <summary>
/// Тело /health и /ready (при готовности).
/// </summary>
/// <param name="Status">"Healthy" для /health; "Ready" для /ready.</param>
public sealed record HealthResponse(string Status);
