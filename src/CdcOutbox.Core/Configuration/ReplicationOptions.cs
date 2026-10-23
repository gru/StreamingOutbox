using System.ComponentModel.DataAnnotations;

namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Объекты логической репликации PostgreSQL.
/// </summary>
public sealed class ReplicationOptions
{
    /// <summary>
    /// Путь секции конфигурации.
    /// </summary>
    public const string ConfigurationSection = "CdcOutbox:Replication";

    /// <summary>
    /// Имя failover-слота логической репликации.
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Replication:SlotName не задан.")]
    public string SlotName { get; set; } = string.Empty;

    /// <summary>
    /// Имя publication с outbox-таблицей.
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Replication:PublicationName не задана.")]
    public string PublicationName { get; set; } = string.Empty;
}
