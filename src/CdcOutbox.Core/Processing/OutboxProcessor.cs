using CdcOutbox.Core.Metrics;
using Microsoft.Extensions.Hosting;

namespace CdcOutbox.Core.Processing;

/// <summary>
/// Цикл outbox-процессора. Конфигурация валидируется стандартным конвейером options
/// при старте хоста, объекты доставки проверяет <see cref="OutboxStartupChecks"/>;
/// необработанное исключение цикла по умолчанию останавливает хост.
/// </summary>
public sealed class OutboxProcessor : BackgroundService
{
    private static readonly TimeSpan IterationDelay = TimeSpan.FromSeconds(1);

    private readonly OutboxMetrics _metrics;

    /// <summary>
    /// Создать процессор.
    /// </summary>
    /// <param name="metrics">Метрики процессора.</param>
    public OutboxProcessor(OutboxMetrics metrics)
    {
        _metrics = metrics;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _metrics.SetActive(true);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _metrics.TouchIteration();
                await Task.Delay(IterationDelay, stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception)
        {
            _metrics.RegisterError();
            throw;
        }
        finally
        {
            _metrics.SetActive(false);
        }
    }
}
