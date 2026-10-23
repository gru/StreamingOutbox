namespace CdcOutbox.IntegrationTests;

public sealed record SlotState(string SlotName, string? Plugin, bool? Failover, string? ConfirmedFlushLsn);
