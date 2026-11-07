using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Redundancy;
using CdcOutbox.Core.Replication;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Kafka;

/// <summary>
/// Публикатор outbox-сообщений в Kafka: события и checkpoint-записи
/// фиксируются транзакциями (<see cref="LeadershipLostException"/> — потеря лидерства).
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
    private readonly TimeSpan _maxRetryDelay = TimeSpan.FromSeconds(processorOptions.Value.MaxRetryDelaySeconds);
    private readonly TimeSpan _checkpointInterval = TimeSpan.FromSeconds(processorOptions.Value.CheckpointIntervalSeconds);

    private DateTimeOffset? _lastCheckpointAt;

    /// <summary>
    /// Публиковать сообщения из канала до его завершения или отмены.
    /// </summary>
    /// <param name="input">Читатель канала сообщений.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Задача публикации.</returns>
    public async Task PublishAsync(ChannelReader<OutboxMessage> input, CancellationToken cancellationToken)
    {
        var retry = new Queue<OutboxMessage>();
        var publishBackoff = new RetryBackoff(_retryDelay, _maxRetryDelay);
        var checkpointBackoff = new RetryBackoff(_retryDelay, _maxRetryDelay);
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    if (!await CycleAsync(input, retry, checkpointBackoff, cancellationToken))
                    {
                        break;
                    }

                    publishBackoff.Reset();
                }
                catch (LeadershipLostException)
                {
                    // сработал fencing продюсера: ретраи бессмысленны, доставка останавливается
                    throw;
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
                    var delay = publishBackoff.NextDelay();
                    logger.LogError(
                        exception,
                        "Публикация прервана: повтор через {DelaySeconds} с.",
                        delay.TotalSeconds);
                    try
                    {
                        await Task.Delay(delay, cancellationToken);
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
            await SaveFinalCheckpointAsync();
        }
    }

    /// <summary>
    /// Один такт цикла публикации; false — канал завершён и очередь повторов пуста.
    /// </summary>
    private async Task<bool> CycleAsync(
        ChannelReader<OutboxMessage> input,
        Queue<OutboxMessage> retry,
        RetryBackoff checkpointBackoff,
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
            // первое сообщение ждём блокирующе; если есть несохранённый прогресс
            // и истекла каденция — ожидание ограничено остатком каденса:
            // по таймауту цикл выходит на checkpoint-only транзакцию
            var idleBudget = IdleCheckpointBudget();
            if (idleBudget is { } dueNow && dueNow <= TimeSpan.Zero)
            {
                await SaveIdleCheckpointAsync(checkpointBackoff, cancellationToken);
                return true;
            }

            var readable = await WaitToReadAsync(input, idleBudget, cancellationToken);
            if (readable is null)
            {
                await SaveIdleCheckpointAsync(checkpointBackoff, cancellationToken);
                return true;
            }

            if (!readable.Value)
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

        await PublishBatchTransactionAsync(batch, retry, cancellationToken);
        metrics.TouchIteration();
        return true;
    }

    private async Task<bool?> WaitToReadAsync(
        ChannelReader<OutboxMessage> input,
        TimeSpan? budget,
        CancellationToken cancellationToken)
    {
        if (budget is not { } remaining)
        {
            return await input.WaitToReadAsync(cancellationToken);
        }

        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bounded.CancelAfter(remaining);
        try
        {
            return await input.WaitToReadAsync(bounded.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // истёк бюджет каденса простоя — выйти на checkpoint-only транзакцию
            return null;
        }
    }

    /// <summary>
    /// Опубликовать батч событий одной транзакцией.
    /// </summary>
    private async Task PublishBatchTransactionAsync(
        List<PendingMessage> batch,
        Queue<OutboxMessage> retry,
        CancellationToken cancellationToken)
    {
        var failed = new List<OutboxMessage>();
        producer.BeginTransaction();
        ulong? deliveredLsn = null;
        var allDelivered = true;
        var checkpointPlanned = false;
        try
        {
            foreach (var pending in batch)
            {
                ProduceMessage(pending, cancellationToken);
            }

            foreach (var pending in batch)
            {
                if (pending.Delivery is null)
                {
                    allDelivered = false;
                    failed.Add(pending.Message);
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
                    failed.Add(pending.Message);
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
                    failed.Add(pending.Message);
                    continue;
                }

                metrics.AdjustPendingEvents(-1);
                metrics.RegisterSentMessage();
                metrics.ObservePublishLatency(Stopwatch.GetElapsedTime(pending.StartedTicks).TotalSeconds);
                metrics.ObserveMessageAge(AgeSeconds(pending.Message.CreatedAt, timeProvider.GetUtcNow().UtcDateTime));
                if (pending.Message.TransactionEndLsn is { } transactionEndLsn
                    && (deliveredLsn is null || transactionEndLsn > deliveredLsn))
                {
                    deliveredLsn = transactionEndLsn;
                }
            }

            // checkpoint едет той же транзакцией, что и события: фиксируются
            // атомарно, позиция — водяной знак полностью доставленного батча
            if (allDelivered && deliveredLsn is { } watermark
                && CheckpointDue() && watermark > progress.CheckpointedPosition)
            {
                await checkpointStore.ProduceInTransactionAsync(watermark, cancellationToken);
                checkpointPlanned = true;
            }

            producer.CommitTransaction(OutboxProducerConfig.TransactionOperationTimeout);
        }
        catch (Exception exception) when (Fencing.IsFenced(exception))
        {
            EnqueueAll(batch, retry);
            TryAbortTransaction();
            throw new LeadershipLostException(
                "Транзакция публикации отклонена: сработал fencing продюсера — лидерство потеряно.",
                exception);
        }
        catch (Exception)
        {
            // commit не прошёл — фиксации нет, весь батч повторяется
            EnqueueAll(batch, retry);
            TryAbortTransaction();
            throw;
        }

        var now = timeProvider.GetUtcNow().UtcDateTime;
        if (deliveredLsn is { } committed)
        {
            progress.MarkDelivered(committed);
        }

        if (checkpointPlanned && deliveredLsn is { } checkpointed)
        {
            progress.MarkCheckpointed(checkpointed, now);
            metrics.RegisterCheckpointSaved();
            _lastCheckpointAt = timeProvider.GetUtcNow();
            logger.LogDebug(
                "Транзакция с checkpoint {Lsn} зафиксирована.",
                LsnText.Format(checkpointed));
        }

        if (!allDelivered)
        {
            foreach (var message in failed)
            {
                retry.Enqueue(message);
            }
        }
    }

    /// <summary>
    /// Остаток ожидания до checkpoint-транзакции простоя (null — ждать нечего).
    /// </summary>
    private TimeSpan? IdleCheckpointBudget() =>
        progress.DeliveredPosition <= progress.CheckpointedPosition
            ? null
            : _lastCheckpointAt is { } last
                ? timeProvider.GetUtcNow() - last >= _checkpointInterval
                    ? TimeSpan.Zero
                    : _checkpointInterval - (timeProvider.GetUtcNow() - last)
                : TimeSpan.Zero;

    private bool CheckpointDue() =>
        _lastCheckpointAt is not { } last || timeProvider.GetUtcNow() - last >= _checkpointInterval;

    /// <summary>
    /// Сохранить несохранённый прогресс при простое потока.
    /// </summary>
    private async Task SaveIdleCheckpointAsync(RetryBackoff checkpointBackoff, CancellationToken cancellationToken)
    {
        var target = progress.DeliveredPosition;
        try
        {
            await checkpointStore.SaveTransactionAsync(target, cancellationToken);
        }
        catch (Exception exception) when (Fencing.IsFenced(exception))
        {
            throw new LeadershipLostException(
                "Сохранение checkpoint отклонено: сработал fencing продюсера — лидерство потеряно.",
                exception);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            metrics.RegisterCheckpointError();
            var delay = checkpointBackoff.NextDelay();
            logger.LogError(
                exception,
                "Сохранение checkpoint {Lsn} не выполнено: повтор через {DelaySeconds} с.",
                LsnText.Format(target),
                delay.TotalSeconds);
            try
            {
                await Task.Delay(delay, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // финальное сохранение канала завершения доделает прогресс
            }

            return;
        }

        progress.MarkCheckpointed(target, timeProvider.GetUtcNow().UtcDateTime);
        metrics.RegisterCheckpointSaved();
        checkpointBackoff.Reset();
        _lastCheckpointAt = timeProvider.GetUtcNow();
    }

    /// <summary>
    /// Финально сохранить несохранённый прогресс при завершении канала.
    /// </summary>
    private async Task SaveFinalCheckpointAsync()
    {
        if (progress.DeliveredPosition <= progress.CheckpointedPosition)
        {
            return;
        }

        var target = progress.DeliveredPosition;
        try
        {
            await checkpointStore.SaveTransactionAsync(target, CancellationToken.None);
            progress.MarkCheckpointed(target, timeProvider.GetUtcNow().UtcDateTime);
            metrics.RegisterCheckpointSaved();
        }
        catch (Exception exception)
        {
            metrics.RegisterCheckpointError();
            logger.LogError(exception, "Финальное сохранение checkpoint {Lsn} не выполнено.", LsnText.Format(target));
        }
    }

    private void EnqueueAll(List<PendingMessage> batch, Queue<OutboxMessage> retry)
    {
        foreach (var pending in batch)
        {
            retry.Enqueue(pending.Message);
        }
    }

    private void TryAbortTransaction()
    {
        try
        {
            producer.AbortTransaction(OutboxProducerConfig.TransactionOperationTimeout);
        }
        catch (Exception exception)
        {
            // откат у заблокированного fencing/отказавшего продюсера может не выполниться —
            // транзакция не зафиксирована, причина важнее
            logger.LogDebug(exception, "Откат незакрытой транзакции публикации не выполнен.");
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
