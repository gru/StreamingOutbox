using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHealthChecks();

var app = builder.Build();

app.Logger.LogInformation("CdcOutbox.Host: скелет без реализации (этап каркаса)");

app.MapHealthChecks("/health");
app.MapHealthChecks("/ready");

app.MapMetrics();

await app.RunAsync();
