using KafkaValidator.Health;
using KafkaValidator.Metrics;
using KafkaValidator.Validation;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace KafkaValidator.UnitTests;

/// <summary>
/// prometheus-registry: KafkaValidatorMetrics пишет в глобальный реестр
/// процесса (см. KafkaValidatorMetricsTests) — эти тесты не параллелятся с ним.
/// </summary>
[Collection("prometheus-registry")]
public class ValidatorEngineHealthCheckTests
{
    private static (ValidatorEngine Engine, HealthCheckContext Context, ValidatorEngineHealthCheck Check) New()
    {
        var engine = new ValidatorEngine(new KafkaValidatorMetrics());
        var check = new ValidatorEngineHealthCheck(engine);
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("validator-engine", check, HealthStatus.Unhealthy, null),
        };
        return (engine, context, check);
    }

    [Fact]
    public async Task Starting_FailureStatusWithState()
    {
        var (_, context, check) = New();

        var result = await check.CheckHealthAsync(context, CancellationToken.None);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("starting", result.Description);
    }

    [Fact]
    public async Task Running_IsHealthy()
    {
        var (engine, context, check) = New();
        engine.MarkRunning();

        var result = await check.CheckHealthAsync(context, CancellationToken.None);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task Failed_FailureStatusWithState()
    {
        var (engine, context, check) = New();
        engine.MarkFailed("boom");

        var result = await check.CheckHealthAsync(context, CancellationToken.None);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("failed", result.Description);
    }

    [Fact]
    public async Task Stopped_FailureStatus()
    {
        var (engine, context, check) = New();
        engine.MarkRunning();
        engine.MarkStopped();

        var result = await check.CheckHealthAsync(context, CancellationToken.None);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }
}
