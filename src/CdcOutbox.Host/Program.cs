using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Redundancy;
using CdcOutbox.Core.Replication;
using Confluent.Kafka;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<CdcOutboxOptions>()
    .Bind(builder.Configuration.GetSection(CdcOutboxOptions.ConfigurationSection))
    .ValidateOnStart();

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
builder.Services.AddSingleton<IValidateOptions<KafkaOptions>, KafkaSecurityOptionsValidator>();

builder.Services.AddOptions<ProcessorOptions>()
    .Bind(builder.Configuration.GetSection(ProcessorOptions.ConfigurationSection))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddOptions<HostOptions>()
    .Configure<IOptions<ProcessorOptions>>(static (hostOptions, processorOptions) =>
        hostOptions.ShutdownTimeout = TimeSpan.FromSeconds(processorOptions.Value.ShutdownTimeoutSeconds));

builder.Services.AddSingleton<IAdminClient>(sp =>
{
    var kafka = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
    return new AdminClientBuilder(OutboxProducerConfig.BuildAdminConfig(kafka)).Build();
});
builder.Services.AddSingleton<IProducer<byte[], byte[]>>(sp =>
{
    var kafka = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
    var slotName = sp.GetRequiredService<IOptions<ReplicationOptions>>().Value.SlotName;
    return new ProducerBuilder<byte[], byte[]>(OutboxProducerConfig.Build(kafka, slotName)).Build();
});
builder.Services.AddSingleton<OutboxSlotProvisioner>();
builder.Services.AddSingleton<PublicationVerifier>();
builder.Services.AddSingleton<CheckpointTopicVerifier>();
builder.Services.AddSingleton<CheckpointStore>();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddScoped<ReplicationProgress>();
builder.Services.AddScoped<OutboxSlotReader>();
builder.Services.AddScoped<IOutboxSlotReader>(sp => sp.GetRequiredService<OutboxSlotReader>());
builder.Services.AddScoped<OutboxPublisher>();
builder.Services.AddScoped<OutboxProcessor>();
builder.Services.AddSingleton<IOutboxStartupChecks, OutboxStartupChecks>();
builder.Services.AddSingleton(sp => new OutboxMetrics(
    Metrics.DefaultRegistry,
    slotName: sp.GetRequiredService<IOptions<ReplicationOptions>>().Value.SlotName));
builder.Services.AddSingleton<LeadershipCoordinator>();
builder.Services.AddHostedService<LeadershipArbiter>();

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
