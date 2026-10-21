using KafkaValidator.Validation;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KafkaValidator.Health;

/// <summary>
/// Готовность валидатора = KafkaFlow-шина запущена (консюмер группы уже читает
/// topic с начала). Брокеры отдельно не опрашиваются: экосистемный пакет
/// AspNetCore.HealthChecks.Kafka проверяет путь ПРОДЮСЕРА (пишет тестовое
/// сообщение в topic при каждом пробе) — для консюмера это side-effect в
/// кластере и не тот сигнал; фреймворк health checks здесь общий с Writer.
/// </summary>
public sealed class ValidatorEngineHealthCheck(ValidatorEngine engine) : IHealthCheck
{
    /// <summary>
    /// running — готов (пустой topic — тоже готов: сверять пока нечего);
    /// иное состояние (starting/stopped/failed) — не готов (503).
    /// </summary>
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken)
    {
        var state = engine.State;
        return Task.FromResult(state == "running"
            ? HealthCheckResult.Healthy(description: $"состояние: {state}")
            : new HealthCheckResult(context.Registration.FailureStatus, description: $"состояние: {state}"));
    }
}
