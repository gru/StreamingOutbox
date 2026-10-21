using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Writer.Configuration;
using Writer.Contracts;
using Writer.Generation;

namespace Writer.Controllers;

/// <summary>
/// Управление генерацией тестовой нагрузки: запуск, мягкая остановка,
/// состояние и итоги прогонов.
/// </summary>
/// <param name="engine">Оркестратор генерации.</param>
/// <param name="options">Конфигурация Writer (дефолты профиля прогона).</param>
[ApiController]
[Route("api/generation")]
public sealed class GenerationController(GenerationEngine engine, IOptions<WriterOptions> options) : ControllerBase
{
    /// <summary>
    /// Запуск прогона; пустое тело — профиль по умолчанию из конфигурации.
    /// 202 — стартовал (Location указывает на state); 400 — невалидный JSON
    /// или некорректная комбинация параметров профиля; 409 — предыдущий
    /// прогон ещё не завершился.
    /// </summary>
    [HttpPost("start")]
    public async Task<IActionResult> Start()
    {
        RunSpec? spec = null;
        if (Request.ContentLength is > 0)
        {
            try { spec = await Request.ReadFromJsonAsync<RunSpec>(); }
            catch (JsonException) { return BadRequest(new ErrorResponse("тело запроса — невалидный JSON")); }
        }

        var errors = new List<string>();
        var profile = LoadProfile.Resolve(options.Value, spec, errors);
        if (profile is null)
            return BadRequest(new ErrorsResponse(errors));

        var (ok, error) = engine.Start(profile);
        return ok
            ? Accepted("/api/generation/state", new StartAcceptedResponse(profile.RunId, profile))
            : Conflict(new ErrorResponse(error));
    }

    /// <summary>
    /// Мягкая остановка: прекратить диспетчеризацию и дождаться in-flight
    /// батчей (до 60 с). 200 — итоги прогона или текущее состояние;
    /// 409 — генерация не выполняется.
    /// </summary>
    [HttpPost("stop")]
    public async Task<IActionResult> Stop()
    {
        if (!engine.IsRunning)
            return Conflict(new ErrorResponse($"генерация не выполняется (state={engine.State})"));
        var results = await engine.StopAsync(TimeSpan.FromSeconds(60));
        return Ok(results ?? (object)new StateResponse(engine.State));
    }

    /// <summary>
    /// Живой снимок состояния генерации (idle/running/stopping/…).
    /// </summary>
    [HttpGet("state")]
    public IActionResult GetState() => Ok(engine.GetState());

    /// <summary>
    /// Итоги последнего завершённого прогона; 404 — завершённых прогонов ещё нет.
    /// </summary>
    [HttpGet("results")]
    public IActionResult GetResults() =>
        engine.LastResults is { } r ? Ok(r) : NotFound(new ErrorResponse("завершённых прогонов ещё нет"));

    /// <summary>
    /// Ожидаемый набор id последнего завершённого прогона (для KafkaValidator);
    /// 404 — завершённых прогонов ещё нет.
    /// </summary>
    [HttpGet("results/ids")]
    public IActionResult GetResultIds() =>
        engine.GetLastIds() is { } ids ? Ok(ids) : NotFound(new ErrorResponse("завершённых прогонов ещё нет"));
}
