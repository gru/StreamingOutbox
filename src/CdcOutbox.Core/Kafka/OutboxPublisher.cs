using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Replication;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Kafka;

/// <summary>
/// Публикатор outbox-сообщений в Kafka.
/// </summary>
public sealed class OutboxPublisher(
    IProducer<byte[], byte[]> producer,
    IOptions<ProcessorOptions> processorOptions,
    TimeProvider timeProvider,
    OutboxMetrics metrics,
    ReplicationProgress progress,
    CheckpointStore checkpointStore,
    ILogger<OutboxPublisher> logger)
{
    private readonly int _batchSize = processorOptions.Value.BatchSize;
    private readonly TimeSpan _retryDelay = TimeSpan.FromSeconds(processorOptions.Value.RetryDelaySeconds);
    private readonly TimeSpan _checkpointInterval = TimeSpan.FromSeconds(processorOptions.Value.CheckpointIntervalSeconds);

    /// <summary>
    /// Публиковать сообщения из канала до его завершения или отмены;
    /// подтверждённо доставленный прогресс периодически сохраняется
    /// checkpoint-ом, при завершении канала — финальным сохранением.
    /// </summary>
    /// <param name="input">Читатель канала сообщений.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Задача публикации.</returns>
    public async Task PublishAsync(ChannelReader<OutboxMessage> input, CancellationToken cancellationToken)
    {
        var retry = new Queue<OutboxMessage>();
        using var checkpointCts = new CancellationTokenSource();
        // checkpoint-цикл запускается без await и живёт параллельно публикации:
        // он ждёт водяной знак, который двигает только цикл публикации ниже
        var checkpointing = SaveCheckpointsAsync(checkpointCts.Token);
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    if (!await PublishBatchAsync(input, retry, cancellationToken))
                    {
                        break;
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (ChannelClosedException)
                {
                    break;
                }
                catch (Exception exception)
                {
                    metrics.RegisterError();
                    logger.LogError(
                        exception,
                        "Публикация прервана: повтор через {RetryDelaySeconds} с.",
                        _retryDelay.TotalSeconds);
                    try
                    {
                        await Task.Delay(_retryDelay, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            await checkpointCts.CancelAsync();
            // после отмены цикл успевает сохранить несохранённый прогресс:
            // вышли из PublishAsync — всё доставлено и сохранено checkpoint'ом
            await checkpointing;
        }
    }

    private async Task<bool> PublishBatchAsync(
        ChannelReader<OutboxMessage> input,
        Queue<OutboxMessage> retry,
        CancellationToken cancellationToken)
    {
        var batch = new List<PendingMessage>(_batchSize);
        if (retry.Count > 0)
        {
            // повторы раньше новых: старшие сообщения не обгоняются младшими
            await Task.Delay(_retryDelay, cancellationToken);
            while (retry.Count > 0)
            {
                batch.Add(new PendingMessage(retry.Dequeue()));
            }
        }
        else
        {
            // первое сообщение ждём блокирующе — поток пуст, ожидание без занятости
            if (!await input.WaitToReadAsync(cancellationToken))
            {
                return false;
            }

            batch.Add(new PendingMessage(await input.ReadAsync(cancellationToken)));
        }

        // добор батча без ожидания: librdkafka склеивает записи в сетевые батчи,
        // пока не истёк linger, — отдаём ему пачку подряд; TryRead не ждёт:
        // канал пуст — отправляем накопленное сразу, не удерживая сообщения
        while (batch.Count < _batchSize && input.TryRead(out var message))
        {
            batch.Add(new PendingMessage(message));
        }

        for (var index = 0; index < batch.Count; index++)
        {
            ProduceMessage(batch[index], cancellationToken);
        }

        var allDelivered = true;
        ulong? deliveredLsn = null;
        foreach (var pending in batch)
        {
            if (pending.Delivery is null)
            {
                allDelivered = false;
                retry.Enqueue(pending.Message);
                continue;
            }

            try
            {
                await pending.Delivery;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                metrics.AdjustPendingEvents(-1);
                allDelivered = false;
                retry.Enqueue(pending.Message);
                continue;
            }
            catch (Exception exception)
            {
                metrics.AdjustPendingEvents(-1);
                metrics.RegisterError();
                logger.LogError(
                    exception,
                    "Подтверждение доставки сообщения {MessageId} в топик {Topic} не получено: повтор через {RetryDelaySeconds} с.",
                    pending.Message.Id,
                    pending.Message.Topic,
                    _retryDelay.TotalSeconds);
                allDelivered = false;
                retry.Enqueue(pending.Message);
                continue;
            }

            metrics.AdjustPendingEvents(-1);
            metrics.RegisterSentMessage();
            metrics.ObservePublishLatency(ElapsedSeconds(pending.StartedTicks));
            metrics.ObserveMessageAge(AgeSeconds(pending.Message.CreatedAt, timeProvider.GetUtcNow().UtcDateTime));
            if (pending.Message.TransactionEndLsn is { } transactionEndLsn
                && (deliveredLsn is null || transactionEndLsn > deliveredLsn))
            {
                deliveredLsn = transactionEndLsn;
            }
        }

        if (allDelivered && deliveredLsn is { } watermark)
        {
            progress.MarkDelivered(watermark);
        }

        metrics.TouchIteration();
        return true;
    }

    private async Task SaveCheckpointsAsync(CancellationToken cancellationToken)
    {
        // null — ещё не сохраняли: первое сохранение сразу при первом прогрессе,
        // дальше — не чаще интервала каденции
        DateTimeOffset? lastSaveAt = null;
        while (!cancellationToken.IsCancellationRequested)
        {
            if (progress.DeliveredPosition <= progress.CheckpointedPosition)
            {
                try
                {
                    await progress.WaitForDeliveryAsync(_checkpointInterval, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                continue;
            }

            if (lastSaveAt is { } savedAt)
            {
                var sinceLastSave = timeProvider.GetUtcNow() - savedAt;
                if (sinceLastSave < _checkpointInterval)
                {
                    try
                    {
                        await Task.Delay(_checkpointInterval - sinceLastSave, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    continue;
                }
            }

            var target = progress.DeliveredPosition;
            try
            {
                await checkpointStore.SaveAsync(target, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                metrics.RegisterCheckpointError();
                logger.LogError(
                    exception,
                    "Сохранение checkpoint {Lsn} не выполнено: повтор через {RetryDelaySeconds} с.",
                    LsnText.Format(target),
                    _retryDelay.TotalSeconds);
                try
                {
                    await Task.Delay(_retryDelay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                continue;
            }

            progress.MarkCheckpointed(target, timeProvider.GetUtcNow().UtcDateTime);
            metrics.RegisterCheckpointSaved();
            lastSaveAt = timeProvider.GetUtcNow();
        }

        if (progress.DeliveredPosition > progress.CheckpointedPosition)
        {
            var target = progress.DeliveredPosition;
            try
            {
                await checkpointStore.SaveAsync(target, CancellationToken.None);
                progress.MarkCheckpointed(target, timeProvider.GetUtcNow().UtcDateTime);
                metrics.RegisterCheckpointSaved();
            }
            catch (Exception exception)
            {
                metrics.RegisterCheckpointError();
                logger.LogError(exception, "Финальное сохранение checkpoint {Lsn} не выполнено.", LsnText.Format(target));
            }
        }
    }

    private void ProduceMessage(PendingMessage pending, CancellationToken cancellationToken)
    {
        try
        {
            var message = new Message<byte[], byte[]>
            {
                Key = pending.Message.Key,
                Value = pending.Message.Body,
                Headers = ParseHeaders(pending.Message.Headers),
                Timestamp = new Timestamp(pending.Message.CreatedAt, TimestampType.CreateTime),
            };
            pending.StartedTicks = Stopwatch.GetTimestamp();
            metrics.AdjustPendingEvents(1);
            pending.Delivery = pending.Message.Partition is { } partition
                ? producer.ProduceAsync(
                    new TopicPartition(pending.Message.Topic, new Partition(partition)),
                    message,
                    cancellationToken)
                : producer.ProduceAsync(pending.Message.Topic, message, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            metrics.AdjustPendingEvents(-1);
            throw;
        }
        catch (FormatException exception)
        {
            metrics.RegisterError();
            logger.LogError(
                exception,
                "Заголовки сообщения {MessageId} не разбираются: '{Headers}' — сообщение задержано, публикация остановлена на нём.",
                pending.Message.Id,
                pending.Message.Headers);
        }
        catch (Exception exception)
        {
            metrics.AdjustPendingEvents(-1);
            metrics.RegisterError();
            logger.LogError(
                exception,
                "Отправка сообщения {MessageId} в топик {Topic} не принята: повтор через {RetryDelaySeconds} с.",
                pending.Message.Id,
                pending.Message.Topic,
                _retryDelay.TotalSeconds);
        }
    }

    private static Headers ParseHeaders(string value)
    {
        Dictionary<string, string>? parsed;
        try
        {
            parsed = JsonSerializer.Deserialize<Dictionary<string, string>>(value);
        }
        catch (JsonException exception)
        {
            throw new FormatException("Колонка headers: ожидается JSON-объект строковых значений.", exception);
        }

        if (parsed is null)
        {
            throw new FormatException("Колонка headers: ожидается JSON-объект строковых значений.");
        }

        var headers = new Headers();
        foreach (var (name, headerValue) in parsed)
        {
            headers.Add(name, Encoding.UTF8.GetBytes(headerValue));
        }

        return headers;
    }

    private static double ElapsedSeconds(long startedTicks)
    {
        return (Stopwatch.GetTimestamp() - startedTicks) / (double)Stopwatch.Frequency;
    }

    private static double AgeSeconds(DateTime createdAt, DateTime now)
    {
        var age = now - createdAt;
        return age.TotalSeconds < 0 ? 0 : age.TotalSeconds;
    }

    private sealed class PendingMessage(OutboxMessage message)
    {
        public OutboxMessage Message { get; } = message;

        public Task<DeliveryResult<byte[], byte[]>>? Delivery { get; set; }

        public long StartedTicks { get; set; }
    }
}
