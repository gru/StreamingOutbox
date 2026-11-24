namespace CdcOutbox.Core.Partitions;

/// <summary>
/// Состояние защитного курсора доставки партиционированного outbox в PostgreSQL.
/// </summary>
/// <param name="SystemIdentifier">Паспорт кластера PostgreSQL (pg_control_system.system_identifier).</param>
/// <param name="ContiguousDeliveredLsn">Непрерывный подтверждённо доставленный LSN без разрывов.</param>
/// <param name="LastDeliveredCreatedAtUtc">Максимальный created_at среди подтверждённо доставленных сообщений.</param>
/// <param name="RecoveryInProgress">Признак активного восстановления дыры (блокирует удаление любых партиций).</param>
public sealed record PartitionGuardState(
    long SystemIdentifier,
    ulong? ContiguousDeliveredLsn,
    DateTime? LastDeliveredCreatedAtUtc,
    bool RecoveryInProgress);
