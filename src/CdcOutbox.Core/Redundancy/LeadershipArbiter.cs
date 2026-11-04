using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Redundancy;

/// <summary>
/// Единственный hosted-сервис: консюмер контрольной группы на checkpoint-топике,
/// назначение партиции = лидерство (обрабатывает <see cref="LeadershipCoordinator"/>);
/// членство поддерживается всегда, хост живёт и в standby.
/// </summary>
public sealed class LeadershipArbiter(
    LeadershipCoordinator coordinator,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<CdcOutboxOptions> cdcOutboxOptions,
    OutboxMetrics metrics,
    ILogger<LeadershipArbiter> logger) : BackgroundService
{
    private static readonly TimeSpan PollTimeout = TimeSpan.FromSeconds(1);

    private readonly KafkaOptions _kafka = kafkaOptions.Value;

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // свойства процесса, не лидерства: серии обеих метрик есть у каждого
        // экземпляра пары с самого старта (standby экспортирует is_active = 0)
        metrics.SetActive(false);
        metrics.SetRecoveryMode(cdcOutboxOptions.Value.RecoveryMode);

        using var consumer = new ConsumerBuilder<byte[], byte[]>(OutboxProducerConfig.BuildControlConsumerConfig(_kafka))
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
        consumer.Subscribe(_kafka.CheckpointTopic);

        // уступить поток старта хоста: poll-цикл синхронный (Consume), дальше он
        // живёт на пуле потоков, Kestrel и /health стартуют параллельно
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
                    // записи контрольного топика не используются — назначение
                    // партиций и есть арбитраж; регулярный poll поддерживает
                    // членство standby и двигает rebalance-протокол
                    _ = consumer.Consume(PollTimeout);
                }
                catch (ConsumeException exception) when (IsEnvironmentMisconfiguration(exception))
                {
                    throw new InvalidOperationException(
                        $"Контрольная consumer group '{_kafka.ControlGroupId}' не работает в этом окружении: {exception.Error.Reason}. " +
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
            // порядок: остановка доставки (финальный checkpoint) → закрытие консюмера
            // (LeaveGroup — мгновенный rebalance в пользу резерва)
            await coordinator.StopTermAsync();
            consumer.Close();
        }
    }

    /// <summary>
    /// Ошибки окружения, ретраи которых бессмысленны.
    /// </summary>
    private static bool IsEnvironmentMisconfiguration(ConsumeException exception) => exception.Error.Code
        is ErrorCode.InvalidSessionTimeout
        or ErrorCode.GroupAuthorizationFailed
        or ErrorCode.TopicAuthorizationFailed
        or ErrorCode.SaslAuthenticationFailed;
}
