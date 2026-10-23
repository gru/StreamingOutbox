using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Replication;
using Confluent.Kafka;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<PostgresOptions>()
    .Bind(builder.Configuration.GetSection(PostgresOptions.ConfigurationSection))
    .ValidateDataAnnotations()
    .Validate(
        static options => PostgresOptions.FindConnectionIssue(options.ConnectionString) is null,
        "CdcOutbox:Postgres:ConnectionString неполна или не разбирается Npgsql")
    .ValidateOnStart();

builder.Services.AddOptions<ReplicationOptions>()
    .Bind(builder.Configuration.GetSection(ReplicationOptions.ConfigurationSection))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddOptions<OutboxStorageOptions>()
    .Bind(builder.Configuration.GetSection(OutboxStorageOptions.ConfigurationSection))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.ConfigurationSection))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddOptions<ProcessorOptions>()
    .Bind(builder.Configuration.GetSection(ProcessorOptions.ConfigurationSection))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<IAdminClient>(sp =>
{
    var kafka = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
    return new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafka.BootstrapServers }).Build();
});
builder.Services.AddSingleton<OutboxSlotProvisioner>();
builder.Services.AddSingleton<PublicationVerifier>();
builder.Services.AddSingleton<CheckpointTopicVerifier>();
builder.Services.AddSingleton<OutboxSlotReader>();
builder.Services.AddSingleton<IOutboxSlotReader>(sp => sp.GetRequiredService<OutboxSlotReader>());
builder.Services.AddSingleton(new OutboxMetrics(Metrics.DefaultRegistry));
builder.Services.AddHostedService<OutboxStartupChecks>();
builder.Services.AddHostedService<OutboxProcessor>();

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
