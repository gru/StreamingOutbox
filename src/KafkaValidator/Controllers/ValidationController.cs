using System.Text.Json;
using KafkaValidator.Configuration;
using KafkaValidator.Contracts;
using KafkaValidator.Validation;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace KafkaValidator.Controllers;

/// <summary>
/// Валидация доставки: состояние валидатора, сводки прогонов и вердикт по runId.
/// </summary>
/// <param name="engine">Состояние сервиса (движок).</param>
/// <param name="runRegistry">Реестр прогонов по фактическим записям topic.</param>
/// <param name="options">Конфигурация KafkaValidator (config echo снапшота).</param>
/// <param name="service">Вычисление вердикта.</param>
[ApiController]
[Route("api/validation")]
public sealed class ValidationController(
    ValidatorEngine engine,
    RunRegistry runRegistry,
    IOptions<KafkaValidatorOptions> options,
    ValidationService service) : ControllerBase
{
    /// <summary>
    /// Снимок состояния валидатора: состояние движка, счётчики реестра, config echo.
    /// </summary>
    [HttpGet("state")]
    public IActionResult GetState() => Ok(engine.GetSnapshot(runRegistry, options.Value));

    /// <summary>
    /// Сводки всех прогонов, замеченных в topic, в порядке появления.
    /// </summary>
    [HttpGet("runs")]
    public IActionResult ListRuns() => Ok(runRegistry.ListRuns());

    /// <summary>
    /// Сводка одного прогона; 404 — прогон в topic не замечен.
    /// </summary>
    [HttpGet("runs/{runId:guid}")]
    public IActionResult GetRun(Guid runId) =>
        runRegistry.GetRun(runId) is { } summary ? Ok(summary) : NotFound(new RunNotFoundResponse(runId));

    /// <summary>
    /// Вердикт прогона: фактические записи Kafka против ожидаемого набора id
    /// (fetchFromWriter — последний завершённый прогон Writer, иначе — явные массивы).
    /// 200 — вердикт; 400 — некорректный JSON / нет runId / не задан ожидаемый набор;
    /// 409 — последний прогон Writer другой runId; 502 — Writer недоступен.
    /// </summary>
    [HttpPost("validate")]
    public async Task<IActionResult> Validate()
    {
        ValidateRequest? request;
        try
        {
            request = await Request.ReadFromJsonAsync<ValidateRequest>();
        }
        catch (JsonException ex)
        {
            return BadRequest(new ErrorsResponse(new[] { $"некорректный JSON: {ex.Message}" }));
        }
        if (request?.RunId is null)
            return BadRequest(new ErrorsResponse(new[] { "runId обязателен" }));

        try
        {
            return Ok(await service.ValidateAsync(request, Request.HttpContext.RequestAborted));
        }
        catch (WriterUnavailableException ex)
        {
            return Problem(title: "Writer недоступен", detail: ex.Message, statusCode: 502);
        }
        catch (WriterRunMismatchException ex)
        {
            return Problem(title: "runId не совпадает с последним прогоном Writer", detail: ex.Message, statusCode: 409);
        }
        catch (InvalidOperationException ex)
        {
            return BadRequest(new ErrorsResponse(new[] { ex.Message }));
        }
    }
}
