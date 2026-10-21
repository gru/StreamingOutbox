using Microsoft.AspNetCore.Mvc;
using Npgsql;
using Writer.Configuration;
using Writer.Contracts;
using Writer.Generation;

namespace Writer.Controllers;

/// <summary>
/// Health-эндпоинты: /health — процесс жив; /ready — БД отвечает.
/// </summary>
/// <param name="db">Писатель батчей (источник подключения).</param>
[ApiController]
[Route("")]
public sealed class HealthController(OutboxWriter db) : ControllerBase
{
    /// <summary>
    /// Процесс жив; БД не опрашивается.
    /// </summary>
    [HttpGet("health")]
    public IActionResult Health() => Ok(new HealthResponse("Healthy"));

    /// <summary>
    /// SELECT 1 с бюджетом 3 с (прав на таблицы не требует, годится для sys_writer).
    /// 200 — готов; 503 — БД недоступна.
    /// </summary>
    [HttpGet("ready")]
    public async Task<IActionResult> Ready()
    {
        try
        {
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            await using var conn = await db.Source.OpenConnectionAsync(timeoutCts.Token);
            await using var cmd = new NpgsqlCommand("SELECT 1", conn);
            await cmd.ExecuteScalarAsync(timeoutCts.Token);
            return Ok(new HealthResponse("Ready"));
        }
        catch (Exception ex)
        {
            return Problem(detail: ex.Message, statusCode: 503, title: "Not Ready");
        }
    }
}
