namespace CdcOutbox.Core.Replication;

/// <summary>
/// Запись слота в каталоге pg_replication_slots.
/// </summary>
/// <param name="Plugin">Плагин слота; null, если в каталоге пусто.</param>
/// <param name="Failover">Флаг failover слота; null, если в каталоге пусто.</param>
/// <param name="WalStatus">Статус WAL слота (reserved, extended, unreserved, lost); null, если не читался.</param>
/// <param name="InvalidationReason">Причина инвалидации слота (например wal_removed); null, если слот валиден.</param>
public sealed record SlotCatalogEntry(
    string? Plugin,
    bool? Failover,
    string? WalStatus = null,
    string? InvalidationReason = null)
{
    /// <summary>
    /// Признак того, что слот инвалидирован PostgreSQL и больше не может отдавать WAL.
    /// </summary>
    public bool IsInvalidated =>
        string.Equals(WalStatus, "lost", StringComparison.OrdinalIgnoreCase)
        || !string.IsNullOrWhiteSpace(InvalidationReason);
}
