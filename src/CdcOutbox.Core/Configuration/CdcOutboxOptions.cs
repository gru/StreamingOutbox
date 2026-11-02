namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Корневые параметры сервиса outbox.
/// </summary>
public sealed class CdcOutboxOptions
{
    /// <summary>
    /// Путь секции конфигурации.
    /// </summary>
    public const string ConfigurationSection = "CdcOutbox";

    /// <summary>
    /// Recovery Mode — ручное аварийное восстановление на один старт: доставка продолжается
    /// с текущей позиции слота (отсутствующий слот создаётся с текущей позиции WAL), новый
    /// checkpoint сохраняется до начала чтения; события между старой и новой позицией
    /// пропускаются осознанно.
    /// </summary>
    public bool RecoveryMode { get; set; }
}
