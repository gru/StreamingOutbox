using KafkaFlow;
using KafkaValidator.Metrics;
using KafkaValidator.Validation;
using Microsoft.Extensions.Options;

namespace KafkaValidator.Kafka;

/// <summary>
/// Обработка каждой записи topic: разбор body → счётчики реестра по body.runId.
/// Полная логика вердикта — в VerdictComputer (чистая, unit-тесты); handler
/// максимально тонкий: потребление никогда не падает (битая запись — счётчик
/// parse_errors, не сбой консюмера).
/// </summary>
public sealed class ValidationHandler : IMessageHandler<byte[]>
{
    private readonly RunRegistry _registry;
    private readonly KafkaValidatorMetrics _metrics;

    public ValidationHandler(RunRegistry registry, KafkaValidatorMetrics metrics)
    {
        _registry = registry;
        _metrics = metrics;
    }

    public Task Handle(IMessageContext context, byte[] message)
    {
        _metrics.RecordsConsumed.Inc();

        if (OutboxMessageBody.TryParse(message, out var consumed) is false || consumed is null)
        {
            _metrics.AddParseError("malformed");
            _registry.RecordParseError();
            return Task.CompletedTask;
        }

        if (consumed.RunId is null)
        {
            // валидное тело без runId (ручная запись в outbox) — в прогоны не
            // попадает, но в общем счётчике потребления учтён
            _metrics.AddSkipped("no_run_id");
        }
        _registry.Record(consumed, DateTimeOffset.UtcNow);
        // gauge по реестру (реестр — источник правды, prometheus-net gauge push-based)
        _metrics.ObserveRunsSeen(_registry.RunsSeen);
        return Task.CompletedTask;
    }
}
