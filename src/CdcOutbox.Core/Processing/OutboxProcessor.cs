using System.Diagnostics;
using System.Threading.Channels;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Redundancy;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Processing;

/// <summary>
/// Реализует цикл доставки: чтение из слота и сохранение в Kafka.
/// </summary>
public sealed class OutboxProcessor(
    IOutboxSlotReader reader,
    OutboxPublisher publisher,
    IOptions<ProcessorOptions> processorOptions,
    OutboxMetrics metrics,
    ILogger<OutboxProcessor> logger)
{
    private readonly int _queueCapacity = processorOptions.Value.QueueCapacity;
    private readonly TimeSpan _retryDelay = TimeSpan.FromSeconds(processorOptions.Value.RetryDelaySeconds);
    private readonly TimeSpan _maxRetryDelay = TimeSpan.FromSeconds(processorOptions.Value.MaxRetryDelaySeconds);
    private readonly TimeSpan _stableSessionInterval = TimeSpan.FromSeconds(processorOptions.Value.BackoffResetSessionSeconds);

    /// <summary>
    /// Запустить цикл доставки.
    /// </summary>
    /// <param name="stoppingToken">Токен остановки доставки.</param>
    /// <returns>Задача цикла.</returns>
    public async Task RunAsync(CancellationToken stoppingToken)
    {
        var channel = Channel.CreateBounded<OutboxMessage>(
            new BoundedChannelOptions(_queueCapacity) { SingleWriter = true });

        // публикация: отменяется последней — после дренирования канала
        using var publisherCts = new CancellationTokenSource();
        // чтение: отменяется остановкой доставки или сбоем публикации
        using var faultCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        var publishing = publisher.PublishAsync(channel.Reader, publisherCts.Token);

        _ = CancelReadingOnPublishFailureAsync(publishing, faultCts);

        try
        {
            var backoff = new RetryBackoff(_retryDelay, _maxRetryDelay);
            var session = 0;

            metrics.TouchIteration();

            while (!faultCts.Token.IsCancellationRequested)
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
                    await reader.ReadAsync(channel.Writer, faultCts.Token);
                }
                catch (OperationCanceledException) when (faultCts.Token.IsCancellationRequested)
                {
                    // отмена (потеря лидерства или сбой публикации) — штатный выход без метрики ошибок
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
                    metrics.RegisterError();

                    HandleReadingFailure(error, delay);
                }

                try
                {
                    await Task.Delay(delay, faultCts.Token);
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
            catch (LeadershipLostException)
            {
                // fencing наружу: координатор остановит доставку без финального сохранения
                throw;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Цикл публикации завершился с ошибкой.");
            }

            await publisherCts.CancelAsync();
        }
    }

    private void HandleReadingFailure(Exception error, TimeSpan delay)
    {
        var issue = OutboxSlotReader.FindReadingIssue(error);
        if (issue.IsTerminal)
        {
            logger.LogCritical(
                error,
                "Чтение слота остановлено: {Issue} Хост завершается — повторные попытки бессмысленны.",
                issue.Description);

            throw new InvalidOperationException($"Чтение слота остановлено: {issue.Description}", error);
        }

        logger.LogError(
            error,
            "Чтение слота прервано: {Issue} Повтор через {DelaySeconds} с.",
            issue.Description,
            delay.TotalSeconds);
    }

    private static async Task CancelReadingOnPublishFailureAsync(Task publishing, CancellationTokenSource readingCts)
    {
        try
        {
            await publishing;
        }
        catch (Exception)
        {
            // ошибку пробрасывает RunAsync (await publishing в finally) — здесь только отмена чтения
            await readingCts.CancelAsync();
        }
    }
}
