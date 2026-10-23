using System.Threading.Channels;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Processing;

/// <summary>
/// Цикл outbox-процессора: чтение слота в bounded-канал (backpressure)
/// с петлёй восстановления — сервис не зависит от рестарт-политики среды
/// запуска: ошибка чтения (сеть, слот, декодирование) регистрируется
/// метрикой и журналом, после паузы попытка повторяется; позиция слота
/// не подтверждается, поэтому повтор ничего не теряет. Отмена — штатный
/// выход без метрики ошибок.
/// </summary>
public sealed class OutboxProcessor : BackgroundService
{
    private readonly IOutboxSlotReader _reader;
    private readonly int _queueCapacity;
    private readonly TimeSpan _retryDelay;
    private readonly OutboxMetrics _metrics;
    private readonly ILogger<OutboxProcessor> _logger;

    /// <summary>
    /// Создать процессор.
    /// </summary>
    /// <param name="reader">Читатель слота.</param>
    /// <param name="processorOptions">Параметры цикла.</param>
    /// <param name="metrics">Метрики процессора.</param>
    /// <param name="logger">Журнал.</param>
    public OutboxProcessor(
        IOutboxSlotReader reader,
        IOptions<ProcessorOptions> processorOptions,
        OutboxMetrics metrics,
        ILogger<OutboxProcessor> logger)
    {
        _reader = reader;
        _queueCapacity = processorOptions.Value.QueueCapacity;
        _retryDelay = TimeSpan.FromSeconds(processorOptions.Value.RetryDelaySeconds);
        _metrics = metrics;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _metrics.SetActive(true);
        var channel = Channel.CreateBounded<OutboxMessage>(
            new BoundedChannelOptions(_queueCapacity) { SingleWriter = true });
        try
        {
            _metrics.TouchIteration();
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await _reader.ReadAsync(channel.Writer, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    _metrics.RegisterError();
                    _logger.LogError(
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
            _metrics.SetActive(false);
        }
    }
}
