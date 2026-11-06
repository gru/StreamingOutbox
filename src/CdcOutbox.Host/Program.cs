using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddCdcOutbox(builder.Configuration);

builder.Services.AddOptions<HostOptions>()
    .Configure<IOptions<ProcessorOptions>>(static (hostOptions, processorOptions) =>
        hostOptions.ShutdownTimeout = TimeSpan.FromSeconds(processorOptions.Value.ShutdownTimeoutSeconds));

builder.Services.AddHealthChecks()
    .AddNpgSql(
        sp => sp.GetRequiredService<IOptions<PostgresOptions>>().Value.ConnectionString,
        name: "postgres",
        timeout: TimeSpan.FromSeconds(3));

var app = builder.Build();

app.MapHealthChecks("/health", new HealthCheckOptions
{
    Predicate = _ => false,
});

app.MapHealthChecks("/ready");

app.MapMetrics();

await app.RunAsync();
