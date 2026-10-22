using System.ComponentModel.DataAnnotations;

namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Параметры Kafka: кластер, продюсер событий и checkpoint-стор.
/// </summary>
public sealed class KafkaOptions
{
    /// <summary>
    /// Путь секции конфигурации.
    /// </summary>
    public const string ConfigurationSection = "CdcOutbox:Kafka";

    /// <summary>
    /// Список bootstrap-серверов Kafka.
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Kafka:BootstrapServers не задан.")]
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Режим подтверждения записи (ProducerConfig Acks).
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Kafka:Acks не задан.")]
    public string Acks { get; set; } = "all";

    /// <summary>
    /// Идемпотентный продюсер (ProducerConfig EnableIdempotence).
    /// </summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>
    /// Батчинг продюсера: сообщений на батч (ProducerConfig BatchNumMessages).
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:BatchNumMessages должен быть положительным.")]
    public int BatchNumMessages { get; set; } = 10000;

    /// <summary>
    /// Задержка накопления батча, мс (ProducerConfig LingerMs).
    /// </summary>
    [Range(0, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:LingerMs не может быть отрицательным.")]
    public int LingerMs { get; set; } = 5;

    /// <summary>
    /// Таймаут доставки сообщения, включая ретраи, мс (ProducerConfig DeliveryTimeoutMs).
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:DeliveryTimeoutMs должен быть положительным.")]
    public int DeliveryTimeoutMs { get; set; } = 120000;

    /// <summary>
    /// Лимит ретраев отправки (ProducerConfig MessageSendMaxRetries).
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:MessageSendMaxRetries должен быть положительным.")]
    public int MessageSendMaxRetries { get; set; } = int.MaxValue;

    /// <summary>
    /// Максимальный размер сообщения, байт (ProducerConfig MessageMaxBytes).
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:MessageMaxBytes должен быть положительным.")]
    public int MessageMaxBytes { get; set; } = 1000000;

    /// <summary>
    /// Служебный topic checkpoint-стора (log compaction).
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Kafka:CheckpointTopic не задан.")]
    public string CheckpointTopic { get; set; } = "cdc-checkpoints";
}
