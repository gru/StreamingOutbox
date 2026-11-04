using Confluent.Kafka;
using KafkaValidator.Configuration;
using Microsoft.Extensions.Options;

namespace KafkaValidator.Kafka;

/// <summary>
/// Hosted-служба: консюмер группы kafka-validator topic'а доставки.
/// Оффсеты никогда не коммитятся — каждый старт процесса перематывает topic
/// с начала и полностью восстанавливает счётчики всех прогонов. Транспортная
/// ошибка потребления не роняет процесс: запись логируется, цикл продолжается.
/// </summary>
public sealed class ValidatorConsumerService(
    IOptions<KafkaValidatorOptions> options,
    ValidationHandler handler,
    ILogger<ValidatorConsumerService> logger) : BackgroundService
{
    /// <summary>
    /// Цикл потребления: одна запись topic → обработка в ValidationHandler.
    /// </summary>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kafka = options.Value.Kafka;
        var config = new ConsumerConfig
        {
            BootstrapServers = kafka.Brokers,
            GroupId = kafka.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            // резервирование Active-Passive: CDC публикует события транзакциями,
            // читаем только зафиксированное
            IsolationLevel = IsolationLevel.ReadCommitted,
        };

        return Task.Run(
            () =>
            {
                using var consumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
                consumer.Subscribe(kafka.Topic);
                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            var result = consumer.Consume(stoppingToken);
                            handler.Handle(result.Message.Value);
                        }
                        catch (ConsumeException exception)
                        {
                            logger.LogError(exception, "Ошибка потребления записи Kafka — потребление продолжается");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
                finally
                {
                    consumer.Close();
                }
            },
            stoppingToken);
    }
}
