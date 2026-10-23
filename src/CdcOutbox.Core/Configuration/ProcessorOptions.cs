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
    /// Пауза между попытками восстановления после ошибки чтения слота, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:RetryDelaySeconds должен быть положительным.")]
    public int RetryDelaySeconds { get; set; } = 5;
}
