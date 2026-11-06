namespace KafkaValidator.Validation;

/// <summary>
/// Тестовое тело сообщения, разобранное из записи Kafka.
/// Идентичность — id (= bigint id строки outbox.messages); группировка прогонов — runId.
/// </summary>
public sealed record ConsumedMessage(long Id, Guid? RunId, string RawJson, DateTimeOffset TimestampUtc);
