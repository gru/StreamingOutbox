namespace CdcOutbox.Core.Replication;

/// <summary>
/// План колонок outbox-таблицы.
/// </summary>
/// <param name="Slots">Слот каждой колонки relation.</param>
/// <param name="TypeIds">OID типов колонок из Relation-записи (0 у игнорируемых).</param>
internal sealed record OutboxColumnPlan(OutboxColumnSlot[] Slots, uint[] TypeIds);
