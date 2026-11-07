using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Redundancy;

/// <summary>
/// Hosted сервис, который держит консюмер контрольной группы на checkpoint-топике,
/// назначение партиции = лидерство.
/// </summary>
public sealed class LeadershipArbiter(
    LeadershipCoordinator coordinator,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<ReplicationOptions> replicationOptions,
    IOptions<CdcOutboxOptions> cdcOutboxOptions,
    OutboxMetrics metrics,
    ILogger<LeadershipArbiter> logger) : BackgroundService
{
    private static readonly TimeSpan PollTimeout = TimeSpan.FromSeconds(1);

    private readonly KafkaOptions _kafka = kafkaOptions.Value;

    private readonly string _slotName = replicationOptions.Value.SlotName;

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        metrics.SetActive(false);
        metrics.SetRecoveryMode(cdcOutboxOptions.Value.RecoveryMode);

        using var consumer = BuildControlConsumer(stoppingToken);
        consumer.Subscribe(_kafka.CheckpointTopic);

        await Task.Yield();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                if (coordinator.Fatal.IsCompleted)
                {
                    // терминальный сбой доставки: пробрасываем — хост останавливается
                    await coordinator.Fatal;
                }

                try
                {
                    // записи служебного топика не используются — назначение
                    // партиций и есть арбитраж; регулярный poll поддерживает
                    // членство standby и двигает rebalance-протокол
                    _ = consumer.Consume(PollTimeout);
                }
                catch (ConsumeException exception) when (IsEnvironmentMisconfiguration(exception))
                {
                    throw new InvalidOperationException(
                        $"Контрольная consumer group '{OutboxProducerConfig.BuildControlGroupId(_slotName)}' не работает в этом окружении: {exception.Error.Reason}. " +
                        "Проверьте границы параметров группы окружения (session timeout) и права принципала.",
                        exception);
                }
                catch (ConsumeException exception)
                {
                    logger.LogError(exception, "Опрос контрольной группы не удался: повтор следующим тактом.");
                }
            }
        }
        finally
        {
            await coordinator.StopTermAsync();
            consumer.Close();
        }
    }

    private IConsumer<byte[], byte[]> BuildControlConsumer(CancellationToken stoppingToken)
    {
        return new ConsumerBuilder<byte[], byte[]>(OutboxProducerConfig.BuildControlConsumerConfig(_kafka, _slotName))
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                try
                {
                    coordinator.HandlePartitionsAssigned(partitions, stoppingToken);
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, "Обработка назначения партиций контрольной группы не удалась.");
                }
            })
            .SetPartitionsRevokedHandler((_, _) => coordinator.HandlePartitionsRevoked())
            .SetPartitionsLostHandler((_, _) => coordinator.HandlePartitionsRevoked())
            .Build();
    }

    private static bool IsEnvironmentMisconfiguration(ConsumeException exception) => exception.Error.Code
        is ErrorCode.InvalidSessionTimeout
        or ErrorCode.GroupAuthorizationFailed
        or ErrorCode.TopicAuthorizationFailed
        or ErrorCode.SaslAuthenticationFailed;
}
