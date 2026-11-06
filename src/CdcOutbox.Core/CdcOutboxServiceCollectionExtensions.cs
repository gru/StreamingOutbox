using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Redundancy;
using CdcOutbox.Core.Replication;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core;

public static class CdcOutboxServiceCollectionExtensions
{
    public static IServiceCollection AddCdcOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<CdcOutboxOptions>()
            .Bind(configuration.GetSection(CdcOutboxOptions.ConfigurationSection))
            .ValidateOnStart();

        services.AddOptions<PostgresOptions>()
            .Bind(configuration.GetSection(PostgresOptions.ConfigurationSection))
            .ValidateDataAnnotations()
            .Validate(
                static options => PostgresOptions.FindConnectionIssue(options.ConnectionString) is null,
                "CdcOutbox:Postgres:ConnectionString неполна или не разбирается Npgsql")
            .ValidateOnStart();

        services.AddOptions<ReplicationOptions>()
            .Bind(configuration.GetSection(ReplicationOptions.ConfigurationSection))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddOptions<OutboxStorageOptions>()
            .Bind(configuration.GetSection(OutboxStorageOptions.ConfigurationSection))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection(KafkaOptions.ConfigurationSection))
            .ValidateDataAnnotations()
            .ValidateOnStart();
        services.AddSingleton<IValidateOptions<KafkaOptions>, KafkaSecurityOptionsValidator>();

        services.AddOptions<ProcessorOptions>()
            .Bind(configuration.GetSection(ProcessorOptions.ConfigurationSection))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddSingleton<IAdminClient>(sp =>
        {
            var kafka = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            return new AdminClientBuilder(OutboxProducerConfig.BuildAdminConfig(kafka)).Build();
        });
        // продюсер — сервис периода лидерства: координатор на каждый период лидерства
        // резолвит свежий экземпляр (повторный InitTransactions на готовом невалиден —
        // state Ready); CheckpointStore и стартовые проверки живут в том же скоупе
        services.AddScoped<IProducer<byte[], byte[]>>(sp =>
        {
            var kafka = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var slotName = sp.GetRequiredService<IOptions<ReplicationOptions>>().Value.SlotName;
            return new ProducerBuilder<byte[], byte[]>(OutboxProducerConfig.Build(kafka, slotName)).Build();
        });
        services.AddSingleton<OutboxSlotProvisioner>();
        services.AddSingleton<PublicationVerifier>();
        services.AddSingleton<CheckpointTopicVerifier>();
        services.AddScoped<CheckpointStore>();
        services.AddSingleton(TimeProvider.System);
        services.AddScoped<ReplicationProgress>();
        services.AddScoped<OutboxSlotReader>();
        services.AddScoped<IOutboxSlotReader>(sp => sp.GetRequiredService<OutboxSlotReader>());
        services.AddScoped<OutboxPublisher>();
        services.AddScoped<OutboxProcessor>();
        services.AddScoped<IOutboxStartupChecks, OutboxStartupChecks>();
        services.AddSingleton(sp => new OutboxMetrics(
            Prometheus.Metrics.DefaultRegistry,
            slotName: sp.GetRequiredService<IOptions<ReplicationOptions>>().Value.SlotName));
        services.AddSingleton<LeadershipCoordinator>();
        services.AddHostedService<LeadershipArbiter>();

        return services;
    }
}
