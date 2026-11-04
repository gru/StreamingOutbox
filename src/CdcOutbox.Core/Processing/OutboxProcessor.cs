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
/// Цикл доставки: чтение слота в bounded-канал (backpressure) и параллельная
/// публикация в Kafka; запуск/остановка — <see cref="Redundancy.LeadershipCoordinator"/>.
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
    /// Прогнать цикл доставки до отмены токена.
    /// </summary>
    /// <param name="stoppingToken">Токен остановки периода лидерства.</param>
    /// <returns>Задача цикла.</returns>
    public async Task RunAsync(CancellationToken stoppingToken)
    {
        var channel = Channel.CreateBounded<OutboxMessage>(
            new BoundedChannelOptions(_queueCapacity) { SingleWriter = true });
        using var publisherCts = new CancellationTokenSource();
        using var faultCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        // публикация останавливается завершением канала, а не отменой
        var publishing = publisher.PublishAsync(channel.Reader, publisherCts.Token);
        // сбой публикации должен останавливать чтение немедленно — иначе читатель
        // держит слот, а ошибка ждёт в незаблюдаемой задаче
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
                logger.LogError(exception, "Петля публикации завершилась с ошибкой.");
            }

            await publisherCts.CancelAsync();
        }
    }

    /// <summary>
    /// Отменить чтение при сбое публикации.
    /// </summary>
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
