using Microsoft.Extensions.Options;
using Prometheus;
using Writer.Configuration;
using Writer.Generation;
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

var app = builder.Build();

app.Logger.LogInformation("Writer запускается; БД: {Host}:{Port}/{Database}",
    app.Configuration["Writer:Connection:Host"] ?? "haproxy",
    app.Configuration["Writer:Connection:Port"] ?? "5432",
    app.Configuration["Writer:Connection:Database:Name"] ?? "outbox");

app.MapControllers();
app.MapMetrics(); // /metrics — pull для Prometheus

await app.RunAsync();
