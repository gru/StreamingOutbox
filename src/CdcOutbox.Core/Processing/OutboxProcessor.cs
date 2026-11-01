using System.Threading.Channels;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Processing;

/// <summary>
/// Цикл outbox-процессора: чтение слота в bounded-канал (backpressure)
/// с петлёй восстановления и параллельной петлёй публикации в Kafka.
/// Сервис не зависит от рестарт-политики среды запуска: ошибка чтения
/// (сеть, слот, декодирование) и ошибка публикации регистрируются
/// метрикой и журналом, после паузы попытка повторяется; позиция слота
/// не подтверждается, поэтому повтор ничего не теряет. Канал живёт дольше
/// сессии чтения — публикация продолжает работу, пока чтение
/// переподключается. Отмена — штатный выход без метрики ошибок.
/// </summary>
public sealed class OutboxProcessor(
    IOutboxSlotReader reader,
    OutboxPublisher publisher,
    IOptions<ProcessorOptions> processorOptions,
    OutboxMetrics metrics,
    ILogger<OutboxProcessor> logger) : BackgroundService
{
    private readonly int _queueCapacity = processorOptions.Value.QueueCapacity;
    private readonly TimeSpan _retryDelay = TimeSpan.FromSeconds(processorOptions.Value.RetryDelaySeconds);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        metrics.SetActive(true);
        var channel = Channel.CreateBounded<OutboxMessage>(
            new BoundedChannelOptions(_queueCapacity) { SingleWriter = true });
        using var publisherCts = new CancellationTokenSource();
        var publishing = publisher.PublishAsync(channel.Reader, publisherCts.Token);
        try
        {
            metrics.TouchIteration();
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await reader.ReadAsync(channel.Writer, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    metrics.RegisterError();
                    logger.LogError(
                        exception,
                        "Чтение слота прервано: {Issue} Повтор через {RetryDelaySeconds} с.",
                        OutboxSlotReader.ClassifyReadingIssue(exception),
                        _retryDelay.TotalSeconds);
                }

                try
                {
                    await Task.Delay(_retryDelay, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        finally
        {
            channel.Writer.Complete();
            try
            {
                await publishing;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Петля публикации завершилась с ошибкой.");
            }

            await publisherCts.CancelAsync();
            metrics.SetActive(false);
        }
    }
}
