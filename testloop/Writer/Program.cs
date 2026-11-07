using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Prometheus;
using Writer.Configuration;
using Writer.Generation;
using Writer.Health;
using Writer.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<WriterOptions>()
    .BindConfiguration(WriterOptions.SectionName);
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<WriterOptions>>().Value);

builder.Services.AddSingleton<WriterMetrics>();
builder.Services.AddSingleton<OutboxWriter>();
builder.Services.AddSingleton<GenerationEngine>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<GenerationEngine>());

builder.Services.AddControllers();

// /ready: SELECT 1 через тот же пул, что и запись (OutboxWriter.Source —
// фабрика резолвится лениво от request-time провайдера, второй пул не создаётся);
// таймаут 3 с — прежний бюджет ready-проверки (HealthCheckRegistration.Timeout,
// снимается фреймворком health checks)
builder.Services.AddHealthChecks()
    .AddNpgSql(
        sp => sp.GetRequiredService<OutboxWriter>().Source,
        name: "postgres",
        timeout: TimeSpan.FromSeconds(3));

var app = builder.Build();

app.Logger.LogInformation("Writer запускается; БД: {Host}:{Port}/{Database}",
    app.Configuration["Writer:Connection:Host"] ?? "haproxy",
    app.Configuration["Writer:Connection:Port"] ?? "5432",
    app.Configuration["Writer:Connection:Database:Name"] ?? "outbox");

app.MapControllers();

// /health — liveness без опроса БД; /ready — чеки выше (postgres)
app.MapHealthChecks("/health", new HealthCheckOptions
{
    Predicate = _ => false,
    ResponseWriter = HealthResponses.WriteAsync,
});
app.MapHealthChecks("/ready", new HealthCheckOptions
{
    ResponseWriter = HealthResponses.WriteAsync,
});

app.MapMetrics(); // /metrics — pull для Prometheus

await app.RunAsync();
