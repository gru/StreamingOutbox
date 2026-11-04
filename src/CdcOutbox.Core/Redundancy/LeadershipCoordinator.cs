using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CdcOutbox.Core.Redundancy;

/// <summary>
/// Управление периодом лидерства Active-Passive: запуск и graceful остановка
/// доставки по назначению/отзыву партиций контрольной группы; терминальный
/// сбой — <see cref="Fatal"/> (остановка хоста).
/// </summary>
public sealed class LeadershipCoordinator(
    IServiceScopeFactory scopeFactory,
    OutboxMetrics metrics,
    ILogger<LeadershipCoordinator> logger)
{
    private static readonly TimeSpan InitTimeout = TimeSpan.FromSeconds(60);

    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly TaskCompletionSource _fatal = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private AsyncServiceScope? _termScope;
    private CancellationTokenSource? _termCts;
    private Task? _deliveryTask;

    /// <summary>
    /// Сбой, который должен остановить хост.
    /// </summary>
    public Task Fatal => _fatal.Task;

    /// <summary>
    /// Является ли этот экземпляр лидером.
    /// </summary>
    public bool IsLeading { get; private set; }

    /// <summary>
    /// Запросить лидерство по назначенным партициям контрольной группы.
    /// </summary>
    /// <param name="partitions">Назначенные партиции checkpoint-топика.</param>
    /// <param name="stoppingToken">Токен остановки хоста.</param>
    public void HandlePartitionsAssigned(IReadOnlyList<TopicPartition> partitions, CancellationToken stoppingToken)
    {
        if (partitions.Count == 0 || IsLeading)
        {
            return;
        }

        _ = GrantAsync(partitions, stoppingToken);
    }

    /// <summary>
    /// Остановить доставку при отзыве или потере партиций.
    /// </summary>
    public void HandlePartitionsRevoked()
    {
        // poll-поток арбитра: блокируется на graceful остановку (группа ждёт
        // ре-join до max.poll.interval.ms)
        try
        {
            StopTermAsync().GetAwaiter().GetResult();
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Остановка доставки при отзыве партиций не удалась.");
        }
    }

    /// <summary>
    /// Остановить доставку лидера; в standby — no-op.
    /// </summary>
    /// <returns>Задача остановки.</returns>
    public async Task StopTermAsync()
    {
        await _gate.WaitAsync();
        try
        {
            await StopTermCoreAsync();
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task GrantAsync(IReadOnlyList<TopicPartition> partitions, CancellationToken stoppingToken)
    {
        await _gate.WaitAsync(stoppingToken);
        AsyncServiceScope? startedScope = null;
        try
        {
            if (IsLeading)
            {
                return;
            }

            metrics.RegisterFailover();
            logger.LogInformation(
                "Получено лидерство (партиции {Partitions} контрольной группы на checkpoint-топике): инициализация транзакционного продюсера — fencing предыдущей эпохи.",
                string.Join(", ", partitions));

            startedScope = scopeFactory.CreateAsyncScope();
            var producer = startedScope.Value.ServiceProvider.GetRequiredService<IProducer<byte[], byte[]>>();
            producer.InitTransactions(InitTimeout);

            var checks = startedScope.Value.ServiceProvider.GetRequiredService<IOutboxStartupChecks>();
            await checks.RunAllAsync(stoppingToken);

            _termCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            var processor = startedScope.Value.ServiceProvider.GetRequiredService<OutboxProcessor>();
            _deliveryTask = processor.RunAsync(_termCts.Token);
            IsLeading = true;
            _termScope = startedScope;
            startedScope = null; // скоуп живёт до конца периода лидерства (StopTermCoreAsync)
            metrics.SetActive(true);
            logger.LogInformation("Лидерство получено: стартовые проверки пройдены, доставка активна.");
            _ = SuperviseDeliveryAsync(_deliveryTask);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // остановка хоста во время получения лидерства — не ошибка
            await DisposeScopeAsync(startedScope);
        }
        catch (LeadershipLostException exception)
        {
            await DisposeScopeAsync(startedScope);
            logger.LogError(
                exception,
                "Запуск доставки заблокирован fencing (параллельный лидер этой эпохи) — возвращаюсь в standby.");
        }
        catch (Exception exception)
        {
            await DisposeScopeAsync(startedScope);
            logger.LogCritical(
                exception,
                "Запуск доставки не удался. Хост завершается — повторные попытки бессмысленны без вмешательства.");
            _fatal.TrySetException(exception);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task SuperviseDeliveryAsync(Task delivery)
    {
        try
        {
            await delivery;
        }
        catch (OperationCanceledException)
        {
            // штатное завершение доставки (отзыв партиций/остановка хоста)
        }
        catch (LeadershipLostException exception)
        {
            logger.LogError(
                exception,
                "Сработал fencing продюсера — лидерство потеряно: доставка остановлена без финального checkpoint, экземпляр остаётся в standby.");
            await StopTermAsync();
        }
        catch (Exception exception)
        {
            // терминальная ошибка доставки (например, инвалидированный слот):
            // Critical залогирован процессором — пробрасываем для остановки хоста
            _fatal.TrySetException(exception);
        }
    }

    private async Task StopTermCoreAsync()
    {
        if (!IsLeading)
        {
            return;
        }

        IsLeading = false;
        metrics.SetActive(false);
        logger.LogInformation("Лидерство потеряно: останавливаю доставку (graceful — финальный checkpoint и подтверждение позиции слота).");
        if (_termCts is { } termCts)
        {
            await termCts.CancelAsync();
        }

        if (_deliveryTask is { } deliveryTask)
        {
            try
            {
                await deliveryTask;
            }
            catch (LeadershipLostException)
            {
                // финальное сохранение уже невозможно: неподтверждённый прогресс
                // пере-доставит новый лидер (at-least-once)
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception exception)
            {
                logger.LogWarning(exception, "Цикл доставки завершился с ошибкой при остановке.");
            }
        }

        _deliveryTask = null;
        if (_termCts is { } expiredCts)
        {
            expiredCts.Dispose();
            _termCts = null;
        }

        // скоуп на период лидерства: позиции/публикатор/процессор освобождаются при его завершении
        await DisposeScopeAsync(_termScope);
        _termScope = null;
    }

    private static async Task DisposeScopeAsync(AsyncServiceScope? scope)
    {
        if (scope is { } active)
        {
            await active.DisposeAsync();
        }
    }
}
