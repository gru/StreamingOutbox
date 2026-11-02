namespace CdcOutbox.Core.Replication;

/// <summary>
/// Запись слота в каталоге pg_replication_slots.
/// </summary>
/// <param name="Plugin">Плагин слота; null, если в каталоге пусто.</param>
/// <param name="Failover">Флаг failover слота; null, если в каталоге пусто.</param>
public sealed record SlotCatalogEntry(string? Plugin, bool? Failover);
