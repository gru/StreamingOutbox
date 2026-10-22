using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Shouldly;
using Xunit;

namespace Writer.UnitTests;

/// <summary>
/// Контракт фреймворка health checks (net8.0), на который опирается /ready:
/// таймаут регистрации (HealthCheckRegistration.Timeout, наш пин — 3 с)
/// снимает зависший чек, а не ждёт его завершения. Проверено эмпирически,
/// т.к. поведение определяется версией фреймворка, а не нашим кодом.
/// </summary>
public class HealthChecksTimeoutTests
{
    private sealed class SlowCheck : IHealthCheck
    {
        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            return HealthCheckResult.Healthy();
        }
    }

    [Fact]
    public async Task RegistrationTimeout_CancelsCheckAndFails()
    {
        var services = new ServiceCollection();
        services.AddLogging(); // DefaultHealthCheckService требует ILogger
        services.AddHealthChecks()
            .AddCheck<SlowCheck>("slow", timeout: TimeSpan.FromMilliseconds(100));
        await using var provider = services.BuildServiceProvider();
        var service = provider.GetRequiredService<HealthCheckService>();

        var stopwatch = Stopwatch.StartNew();
        var report = await service.CheckHealthAsync(CancellationToken.None);
        stopwatch.Stop();

        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(10));
        report.Status.ShouldBe(HealthStatus.Unhealthy);
    }
}
