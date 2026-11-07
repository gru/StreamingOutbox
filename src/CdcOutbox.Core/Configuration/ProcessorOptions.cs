using System.ComponentModel.DataAnnotations;

namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Настройки процессора outbox.
/// </summary>
public sealed class ProcessorOptions
{
    /// <summary>
    /// Путь секции конфигурации.
    /// </summary>
    public const string ConfigurationSection = "CdcOutbox:Processor";

    /// <summary>
    /// Ёмкость очереди чтение→публикация.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:QueueCapacity должен быть положительным.")]
    public int QueueCapacity { get; set; } = 2048;

    /// <summary>
    /// Размер пачки публикации.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:BatchSize должен быть положительным.")]
    public int BatchSize { get; set; } = 500;

    /// <summary>
    /// Интервал сохранения checkpoint, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:CheckpointIntervalSeconds должен быть положительным.")]
    public int CheckpointIntervalSeconds { get; set; } = 5;

    /// <summary>
    /// Порог простоя потока для предупреждения, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:IdleTimeoutSeconds должен быть положительным.")]
    public int IdleTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Начальная пауза повторных попыток, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:RetryDelaySeconds должен быть положительным.")]
    public int RetryDelaySeconds { get; set; } = 5;

    /// <summary>
    /// Максимальная пауза повторных попыток, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:MaxRetryDelaySeconds должен быть положительным.")]
    public int MaxRetryDelaySeconds { get; set; } = 60;

    /// <summary>
    /// Длительность сессии чтения, сбрасывающая счётчик повторов, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:BackoffResetSessionSeconds должен быть положительным.")]
    public int BackoffResetSessionSeconds { get; set; } = 30;

    /// <summary>
    /// Бюджет ожидания финального checkpoint при штатной остановке, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:FinalCheckpointWaitSeconds должен быть положительным.")]
    public int FinalCheckpointWaitSeconds { get; set; } = 15;

    /// <summary>
    /// Бюджет штатной остановки сервиса, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Processor:ShutdownTimeoutSeconds должен быть положительным.")]
    public int ShutdownTimeoutSeconds { get; set; } = 30;
}
