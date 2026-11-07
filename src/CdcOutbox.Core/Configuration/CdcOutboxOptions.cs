namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Настройки сервиса outbox.
/// </summary>
public sealed class CdcOutboxOptions
{
    /// <summary>
    /// Путь секции конфигурации.
    /// </summary>
    public const string ConfigurationSection = "CdcOutbox";

    /// <summary>
    /// Режим ручного аварийного восстановления.
    /// </summary>
    public bool RecoveryMode { get; set; }
}
