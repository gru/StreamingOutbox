using System.Diagnostics;
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
    private readonly TimeSpan _maxRetryDelay = TimeSpan.FromSeconds(processorOptions.Value.MaxRetryDelaySeconds);
    private readonly TimeSpan _stableSessionInterval = TimeSpan.FromSeconds(processorOptions.Value.BackoffResetSessionSeconds);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        metrics.SetActive(true);
        var channel = Channel.CreateBounded<OutboxMessage>(
            new BoundedChannelOptions(_queueCapacity) { SingleWriter = true });
        using var publisherCts = new CancellationTokenSource();
        // публикация останавливается завершением канала, а не отменой
        var publishing = publisher.PublishAsync(channel.Reader, publisherCts.Token);
        try
        {
            var backoff = new RetryBackoff(_retryDelay, _maxRetryDelay);
            var session = 0;
            metrics.TouchIteration();
            while (!stoppingToken.IsCancellationRequested)
            {
                // каждая сессия чтения, кроме первой, — переподключение к слоту
                if (session++ > 0)
                {
                    metrics.RegisterSlotReconnect();
                }

                var sessionStarted = Stopwatch.GetTimestamp();
                Exception? failure = null;
                try
                {
                    // канал живёт дольше сессии: публикация продолжает работать, пока чтение переподключается
                    await reader.ReadAsync(channel.Writer, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // отмена — штатный выход без метрики ошибок
                    break;
                }
                catch (Exception exception)
                {
                    failure = exception;
                }

                // сессия, прожившая порог стабильности, сбрасывает наращивание паузы
                if (Stopwatch.GetElapsedTime(sessionStarted) >= _stableSessionInterval)
                {
                    backoff.Reset();
                }

                var delay = backoff.NextDelay();
                if (failure is { } error)
                {
                    var issue = OutboxSlotReader.FindReadingIssue(error);
                    if (issue.IsTerminal)
                    {
                        // терминальная ошибка (инвалидированный слот): ретраи скрыли бы потерю WAL — останавливаем хост
                        metrics.RegisterError();
                        logger.LogCritical(
                            error,
                            "Чтение слота остановлено: {Issue} Хост завершается — повторные попытки бессмысленны.",
                            issue.Description);
                        throw new InvalidOperationException($"Чтение слота остановлено: {issue.Description}", error);
                    }

                    // транзиентная ошибка: позиция слота не подтверждена — повтор ничего не теряет
                    metrics.RegisterError();
                    logger.LogError(
                        error,
                        "Чтение слота прервано: {Issue} Повтор через {DelaySeconds} с.",
                        issue.Description,
                        delay.TotalSeconds);
                }

                try
                {
                    await Task.Delay(delay, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        finally
        {
            // ридер уже завершил канал при отмене — TryComplete безопасен при любом порядке
            channel.Writer.TryComplete();
            try
            {
                // канал закрыт: публикатор дренирует хвост и сохраняет финальный checkpoint
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
