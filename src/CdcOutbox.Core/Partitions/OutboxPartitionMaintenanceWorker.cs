using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Partitions;

/// <summary>
/// Фоновый менеджер почасовых партиций (запускается только у активного лидера):
/// сразу при старте и далее каждый час ровно в HH:05:00 UTC проверяет запас на 48ч вперёд,
/// досоздаёт недостающие партиции, запечатывает закрытые часы и удаляет доставленные после сдвига LSN.
/// </summary>
public sealed class OutboxPartitionMaintenanceWorker(
    IPartitionRegistryClient registryClient,
    ReplicationProgress progress,
    TimeProvider timeProvider,
    OutboxMetrics metrics,
    IOptions<OutboxStorageOptions> storageOptions,
    IOptions<PartitionMaintenanceOptions> partitionOptions,
    ILogger<OutboxPartitionMaintenanceWorker> logger)
{
    private readonly PartitionMaintenanceOptions _options = partitionOptions.Value;

    /// <summary>
    /// Активен ли менеджер партиций в текущей конфигурации.
    /// </summary>
    public bool IsEnabled { get; } =
        storageOptions.Value.Mode == OutboxStorageMode.PartitionedTable || partitionOptions.Value.Enabled;

    /// <summary>
    /// Запустить цикл обслуживания партиций на время срока лидерства.
    /// </summary>
    /// <param name="cancellationToken">Токен остановки лидерства.</param>
    /// <returns>Задача цикла обслуживания.</returns>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        if (!IsEnabled)
        {
            return;
        }

        // Немедленный прогон при старте лидера, чтобы запас на 48ч появился сразу, не дожидаясь следующего HH:05
        await RunOnceSafelyAsync(cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            var nowUtc = timeProvider.GetUtcNow();
            var nextRunUtc = HourlyPartitionName.CalculateNextRunUtc(nowUtc, _options.RunAtMinuteOfHour);
            var delay = nextRunUtc - nowUtc;
            if (delay < TimeSpan.FromMilliseconds(100))
            {
                delay = TimeSpan.FromMilliseconds(100);
            }

            logger.LogDebug(
                "Следующий плановый запуск обслуживания часовых партиций запланирован на {NextRunUtc:O} (через {Delay}).",
                nextRunUtc,
                delay);

            try
            {
                await Task.Delay(delay, timeProvider, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            await RunOnceSafelyAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Выполнить один полный цикл обслуживания партиций: предсоздание на 48ч → запечатывание → безопасное удаление.
    /// </summary>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Задача выполнения одного цикла.</returns>
    public async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        if (!IsEnabled)
        {
            return;
        }

        var nowUtc = timeProvider.GetUtcNow();

        // 1. Всегда проверяем вперёд на PrecreateHours (по умолчанию 48ч = 2 дня) и досоздаём недостающие
        var ensured = await registryClient.EnsurePartitionsAsync(nowUtc, cancellationToken);
        metrics.SetFuturePartitionsCount(ensured.FuturePartitionsCount);
        metrics.RegisterPartitionsCreated(ensured.CreatedCount);
        if (ensured.CreatedCount > 0)
        {
            logger.LogInformation(
                "Создано {CreatedCount} часовых партиций outbox (запас вперёд: {FutureCount} ч).",
                ensured.CreatedCount,
                ensured.FuturePartitionsCount);
        }

        if (ensured.FuturePartitionsCount < _options.PrecreateHours)
        {
            logger.LogWarning(
                "Запас будущих часовых партиций ({FutureCount} ч) ниже целевого горизонта ({TargetHours} ч).",
                ensured.FuturePartitionsCount,
                _options.PrecreateHours);
        }

        // 2. Запечатываем закрывшиеся часы текущим LSN (после SealGraceMinutes)
        var sealedCount = await registryClient.SealClosedPartitionsAsync(nowUtc, cancellationToken);
        if (sealedCount > 0)
        {
            logger.LogInformation("Запечатано {SealedCount} закрытых часовых партиций outbox.", sealedCount);
        }

        // 3. Фиксируем подтверждённый прогресс доставки и удаляем партиции только при сдвиге LSN
        var systemIdentifier = await registryClient.GetSystemIdentifierAsync(cancellationToken);
        var checkpointedLsn = progress.CheckpointedPosition;
        var lastDeliveredCreatedAt = progress.LastDeliveredCreatedAtUtc;

        await registryClient.EnsureGuardInitializedAsync(
            systemIdentifier,
            checkpointedLsn > 0 ? checkpointedLsn : null,
            lastDeliveredCreatedAt,
            cancellationToken);

        if (checkpointedLsn > 0)
        {
            await registryClient.UpdateDeliveryProgressAsync(
                checkpointedLsn,
                lastDeliveredCreatedAt,
                systemIdentifier,
                cancellationToken);
        }

        var dropped = await registryClient.DropDeliveredPartitionsAsync(
            checkpointedLsn,
            systemIdentifier,
            nowUtc,
            cancellationToken);
        if (dropped.Count > 0)
        {
            metrics.RegisterPartitionsDropped(dropped.Count);
            logger.LogInformation(
                "Удалены доставленные часовые партиции ({Count}): {Partitions}.",
                dropped.Count,
                string.Join(", ", dropped));
        }
    }

    private async Task RunOnceSafelyAsync(CancellationToken cancellationToken)
    {
        try
        {
            await RunOnceAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // штатная остановка лидерства
        }
        catch (Exception exception)
        {
            // сбой фонового обслуживания партиций логируется и отражается в метриках,
            // но не останавливает живой конвейер доставки сообщений
            metrics.RegisterError();
            logger.LogError(exception, "Ошибка цикла обслуживания почасовых партиций outbox.");
        }
    }
}
