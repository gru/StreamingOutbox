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
    /// Буфер KafkaFlow на воркер (записей).
    /// </summary>
    public int BufferSize { get; set; } = 1000;

    /// <summary>
    /// BufferSize, зажатый в [100..100000].
    /// </summary>
    public int ClampBufferSize() => Math.Clamp(BufferSize, 100, 100_000);

    /// <summary>
    /// Число воркеров обработки. По умолчанию 1: записи одного прогона идут
    /// последовально, состояние реестра под lock — параллелизм не нужен.
    /// </summary>
    public int WorkersCount { get; set; } = 1;

    /// <summary>
    /// WorkersCount, зажатый в [1..16].
    /// </summary>
    public int ClampWorkersCount() => Math.Clamp(WorkersCount, 1, 16);

    /// <summary>
    /// Brokers, разобранные в массив (пробелы и пустые элементы отбрасываются).
    /// </summary>
    public string[] BrokerList() =>
        Brokers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
}
