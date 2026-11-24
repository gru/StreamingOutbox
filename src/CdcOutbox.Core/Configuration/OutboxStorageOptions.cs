using System.ComponentModel.DataAnnotations;

namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Настройки хранилища Outbox.
/// </summary>
public class OutboxStorageOptions
{
    /// <summary>
    /// Путь секции конфигурации.
    /// </summary>
    public const string ConfigurationSection = "CdcOutbox:Storage";

    /// <summary>
    /// Схема таблицы outbox.
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Storage:Schema не задана.")]
    public string Schema { get; set; } = "outbox";

    /// <summary>
    /// Имя таблицы outbox.
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Storage:Table не задана.")]
    public string Table { get; set; } = "messages";

    /// <summary>
    /// Режим хранилища: штатная таблица, прямой безтабличный WAL или секционированная таблица-страховка.
    /// </summary>
    public OutboxStorageMode Mode { get; set; } = OutboxStorageMode.Table;

    /// <summary>
    /// Включить декодирование логических WAL-сообщений (pgoutput messages=true)
    /// в дополнение к табличным INSERT — позволяет переключать запись INSERT ↔ WAL без пересоздания слота.
    /// Для <see cref="OutboxStorageMode.WalMessages"/> включается автоматически.
    /// </summary>
    public bool EnableWalMessages { get; set; }

    /// <summary>
    /// Префикс сообщений pg_logical_emit_message, принадлежащих данному outbox-потоку.
    /// </summary>
    public string WalMessagePrefix { get; set; } = "outbox";

    /// <summary>
    /// Активно ли чтение логических WAL-сообщений из слота.
    /// </summary>
    internal bool UsesWalMessages => Mode == OutboxStorageMode.WalMessages || EnableWalMessages;

    /// <summary>
    /// Активен ли табличный путь (требуется проверка таблицы в publication).
    /// </summary>
    internal bool UsesTable => Mode != OutboxStorageMode.WalMessages;
}
