namespace Writer.Contracts;

/// <summary>
/// Тело /health и /ready (MapHealthChecks + HealthResponses.WriteAsync).
/// </summary>
/// <param name="Status">Healthy | Unhealthy (Degraded не используется).</param>
/// <param name="Description">Причина неготовности — описания отказавших чеков; null, когда здоров.</param>
public sealed record HealthResponse(string Status, string? Description = null);
