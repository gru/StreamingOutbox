namespace KafkaValidator.Contracts;

/// <summary>
/// Тело /health и /ready (MapHealthChecks + HealthResponses.WriteAsync).
/// </summary>
/// <param name="Status">Healthy | Unhealthy (Degraded не используется).</param>
/// <param name="Description">Состояние движка при неготовности; null, когда здоров.</param>
public sealed record HealthResponse(string Status, string? Description = null);
