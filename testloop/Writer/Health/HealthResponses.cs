using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Writer.Contracts;

namespace Writer.Health;

/// <summary>
/// Тела /health и /ready (ResponseWriter для MapHealthChecks) в формате
/// контракта HealthResponse: {"status":"Healthy"} или
/// {"status":"Unhealthy","description":"postgres: …"}.
/// Коды ответов ставит health checks middleware: 200 Healthy, 503 Unhealthy.
/// </summary>
internal static class HealthResponses
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>
    /// Записать тело: статус агрегированного отчёта + описания отказавших чеков
    /// (для диагностики; при Healthy описание не пишется).
    /// </summary>
    public static async Task WriteAsync(HttpContext context, HealthReport report)
    {
        var description = report.Status == HealthStatus.Healthy
            ? null
            : string.Join("; ", report.Entries
                .Where(e => e.Value.Status != HealthStatus.Healthy)
                .Select(e => $"{e.Key}: {e.Value.Description ?? e.Value.Exception?.Message}"));
        await context.Response.WriteAsJsonAsync(
            new HealthResponse(report.Status.ToString(), description), JsonOptions);
    }
}
