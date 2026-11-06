using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Redundancy;

/// <summary>
/// Управление периодом лидерства Active-Passive: запуск и graceful остановка
/// доставки по назначению/отзыву партиций контрольной группы; восстановимые
/// ошибки запуска — повторы с backoff, терминальный сбой — <see cref="Fatal"/>
/// (остановка хоста).
/// </summary>
public sealed class LeadershipCoordinator(
    IServiceScopeFactory scopeFactory,
    IOptions<ProcessorOptions> processorOptions,
    OutboxMetrics metrics,
    ILogger<LeadershipCoordinator> logger)
{
    private static readonly TimeSpan InitTimeout = TimeSpan.FromSeconds(60);

    private readonly TimeSpan _retryDelay = TimeSpan.FromSeconds(processorOptions.Value.RetryDelaySeconds);
    private readonly TimeSpan _maxRetryDelay = TimeSpan.FromSeconds(processorOptions.Value.MaxRetryDelaySeconds);
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly TaskCompletionSource _fatal = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private AsyncServiceScope? _termScope;
    private CancellationTokenSource? _termCts;
    private CancellationTokenSource? _grantCts;
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

        CancelGrantAttempts();
        _ = GrantAsync(partitions, stoppingToken);
    }

    /// <summary>
    /// Остановить доставку при отзыве или потере партиций.
    /// </summary>
    public void HandlePartitionsRevoked()
    {
        // poll-поток арбитра: блокируется на graceful остановку (группа ждёт
        // ре-join до max.poll.interval.ms); отмена повторов запуска освобождает
        // gate без ожидания backoff-паузы
        CancelGrantAttempts();
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

            // восстановимые ошибки запуска не останавливают хост (среда может
            // быть без рестарт-политики): повтор с backoff, лидерство
            // удерживается — причина сбоя общая для пары, standby доставил бы
            // не раньше; терминальные нарушения стартовых проверок — Fatal
            var backoff = new RetryBackoff(_retryDelay, _maxRetryDelay);
            using var grantCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            _grantCts = grantCts;
            try
            {
                while (true)
                {
                    AsyncServiceScope? startedScope = null;
                    try
                    {
                        startedScope = scopeFactory.CreateAsyncScope();
                        var producer = startedScope.Value.ServiceProvider.GetRequiredService<IProducer<byte[], byte[]>>();
                        InitProducer(producer);

                        var checks = startedScope.Value.ServiceProvider.GetRequiredService<IOutboxStartupChecks>();
                        await checks.RunAllAsync(grantCts.Token);

                        _termCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                        var processor = startedScope.Value.ServiceProvider.GetRequiredService<OutboxProcessor>();
                        _deliveryTask = processor.RunAsync(_termCts.Token);
                        IsLeading = true;
                        _termScope = startedScope;
                        startedScope = null; // скоуп живёт до конца периода лидерства (StopTermCoreAsync)
                        metrics.SetActive(true);
                        logger.LogInformation("Лидерство получено: стартовые проверки пройдены, доставка активна.");
                        _ = SuperviseDeliveryAsync(_deliveryTask);
                        return;
                    }
                    catch (StartupCheckException exception)
                    {
                        await DisposeScopeAsync(startedScope);
                        logger.LogCritical(
                            exception,
                            "Запуск доставки не удался. Хост завершается — повторные попытки бессмысленны без вмешательства.");
                        _fatal.TrySetException(exception);
                        return;
                    }
                    catch (LeadershipLostException exception)
                    {
                        await DisposeScopeAsync(startedScope);
                        logger.LogError(
                            exception,
                            "Запуск доставки заблокирован fencing (параллельный лидер этой эпохи) — возвращаюсь в standby.");
                        return;
                    }
                    catch (OperationCanceledException)
                    {
                        // остановка хоста или отзыв партиций во время запуска — не ошибка
                        await DisposeScopeAsync(startedScope);
                        return;
                    }
                    catch (Exception exception)
                    {
                        // восстановимая ошибка (сеть и пр.): повтор со свежим
                        // скоупом — продюсер периода лидерства не течёт
                        await DisposeScopeAsync(startedScope);
                        metrics.RegisterError();
                        // нижняя граница 1мс — защита от «горячего» цикла
                        // при нулевой задержке конфига (пауза обязана уступить поток)
                        var delay = TimeSpan.FromMilliseconds(Math.Max(backoff.NextDelay().TotalMilliseconds, 1));
                        logger.LogError(
                            exception,
                            "Запуск доставки прерван: повтор через {DelaySeconds} с.",
                            delay.TotalSeconds);
                        try
                        {
                            await Task.Delay(delay, grantCts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                    }
                }
            }
            finally
            {
                if (ReferenceEquals(_grantCts, grantCts))
                {
                    _grantCts = null;
                }
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Инициализировать транзакционный продюсер; fencing — потеря лидерства, а не сбой запуска.
    /// </summary>
    private static void InitProducer(IProducer<byte[], byte[]> producer)
    {
        try
        {
            producer.InitTransactions(InitTimeout);
        }
        catch (KafkaException exception) when (Fencing.IsFenced(exception))
        {
            throw new LeadershipLostException(
                "Инициализация продюсера заблокирована fencing — параллельный лидер этой эпохи.",
                exception);
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

    /// <summary>
    /// Прервать повторные попытки запуска доставки, если они идут.
    /// </summary>
    private void CancelGrantAttempts()
    {
        try
        {
            _grantCts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // попытки уже завершились
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
