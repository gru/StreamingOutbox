using KafkaValidator.Contracts;
using KafkaValidator.Validation;
using Microsoft.AspNetCore.Mvc;

namespace KafkaValidator.Controllers;

/// <summary>
/// Health-эндпоинты: /health — процесс жив; /ready — KafkaFlow-шина запущена.
/// </summary>
/// <param name="engine">Состояние валидатора (running = готов).</param>
[ApiController]
[Route("")]
public sealed class HealthController(ValidatorEngine engine) : ControllerBase
{
    /// <summary>
    /// Процесс жив; Kafka не опрашивается.
    /// </summary>
    [HttpGet("health")]
    public IActionResult Health() => Ok(new HealthResponse("Healthy"));

    /// <summary>
    /// ready = KafkaFlow-шина запущена (консюмер группы уже читает topic с начала);
    /// пустой topic — тоже ready: сверять пока нечего, но сервис работоспособен.
    /// 503 — состояние не running (starting/stopped/failed).
    /// </summary>
    [HttpGet("ready")]
    public IActionResult Ready() => engine.State == "running"
        ? Ok(new HealthResponse("Ready"))
        : Problem(title: "Not Ready", detail: $"состояние: {engine.State}", statusCode: 503);
}
