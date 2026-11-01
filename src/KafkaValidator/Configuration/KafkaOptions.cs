namespace KafkaValidator.Configuration;

/// <summary>
/// Параметры консюмера Kafka.
/// </summary>
public sealed class KafkaOptions
{
    /// <summary>
    /// Список брокеров через запятую (host:port).
    /// </summary>
    public string Brokers { get; set; } = "kafka1:9092,kafka2:9092,kafka3:9092";

    /// <summary>
    /// Topic с outbox-сообщениями (сообщения тестовой нагрузки Writer).
    /// </summary>
    public string Topic { get; set; } = "outbox.events";

    /// <summary>
    /// Имя consumer-группы (видно в Kafka; оффсеты НЕ коммитятся).
    /// </summary>
    public string GroupId { get; set; } = "kafka-validator";

    /// <summary>
    /// Brokers, разобранные в массив (пробелы и пустые элементы отбрасываются).
    /// </summary>
    public string[] BrokerList() =>
        Brokers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
}
