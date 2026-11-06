namespace CdcOutbox.Core.Replication;

internal sealed class OutboxColumnPlan
{
    internal OutboxColumnPlan(OutboxColumnSlot[] slots, uint[] typeIds)
    {
        Slots = slots;
        TypeIds = typeIds;
    }

    internal OutboxColumnSlot[] Slots { get; }

    /// <summary>
    /// OID типов колонок из Relation-записи (0 у игнорируемых).
    /// </summary>
    internal uint[] TypeIds { get; }
}
