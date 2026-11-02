using System.ComponentModel.DataAnnotations;

namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Параметры цикла процессора outbox.
/// </summary>
public sealed class ProcessorOptions
{
    /// <summary>
    /// Путь секции конфигурации.
    /// </summary>
    public const string ConfigurationSection = "CdcOutbox:Processor";

    /// <summary>
    /// Ёмкость ограниченной очереди чтение→публикация (backpressure).
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:QueueCapacity должен быть положительным.")]
    public int QueueCapacity { get; set; } = 2048;

    /// <summary>
    /// Размер пачки публикации.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:BatchSize должен быть положительным.")]
    public int BatchSize { get; set; } = 500;

    /// <summary>
    /// Интервал периодического сохранения checkpoint, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:CheckpointIntervalSeconds должен быть положительным.")]
    public int CheckpointIntervalSeconds { get; set; } = 5;

    /// <summary>
    /// Порог простоя потока для idle-предупреждения (WARN), с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:IdleTimeoutSeconds должен быть положительным.")]
    public int IdleTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Пауза первой повторной попытки восстановления (база backoff), с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:RetryDelaySeconds должен быть положительным.")]
    public int RetryDelaySeconds { get; set; } = 5;

    /// <summary>
    /// Потолок паузы повторных попыток (backoff), с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:MaxRetryDelaySeconds должен быть положительным.")]
    public int MaxRetryDelaySeconds { get; set; } = 60;

    /// <summary>
    /// Длительность сессии чтения, считающаяся стабильной: такая сессия сбрасывает backoff повторных подключений, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:BackoffResetSessionSeconds должен быть положительным.")]
    public int BackoffResetSessionSeconds { get; set; } = 30;

    /// <summary>
    /// Бюджет ожидания финального checkpoint при штатной остановке, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:FinalCheckpointWaitSeconds должен быть положительным.")]
    public int FinalCheckpointWaitSeconds { get; set; } = 15;

    /// <summary>
    /// Бюджет штатной остановки хоста (дрейн публикации, финальный checkpoint, подтверждение позиции), с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:ShutdownTimeoutSeconds должен быть положительным.")]
    public int ShutdownTimeoutSeconds { get; set; } = 30;
}
