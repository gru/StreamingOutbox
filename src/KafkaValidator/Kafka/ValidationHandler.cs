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
public sealed class ValidationHandler(RunRegistry registry, KafkaValidatorMetrics metrics) : IMessageHandler<byte[]>
{
    /// <summary>
    /// Одна запись topic: разбор body → счётчики реестра по body.runId.
    /// Никогда не бросает исключений — битая запись учитывается parse_errors.
    /// </summary>
    public Task Handle(IMessageContext context, byte[] message)
    {
        metrics.RecordsConsumed.Inc();

        if (OutboxMessageBody.TryParse(message, out var consumed) is false || consumed is null)
        {
            metrics.AddParseError("malformed");
            registry.RecordParseError();
            return Task.CompletedTask;
        }

        if (consumed.RunId is null)
        {
            // валидное тело без runId (ручная запись в outbox) — в прогоны не
            // попадает, но в общем счётчике потребления учтён
            metrics.AddSkipped("no_run_id");
        }
        registry.Record(consumed, DateTimeOffset.UtcNow);
        // gauge по реестру (реестр — источник правды, prometheus-net gauge push-based)
        metrics.ObserveRunsSeen(registry.RunsSeen);
        return Task.CompletedTask;
    }
}
