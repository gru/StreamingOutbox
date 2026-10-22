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
}
