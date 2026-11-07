namespace CdcOutbox.Core.Redundancy;

/// <summary>
/// Лидерство потеряно.
/// </summary>
public sealed class LeadershipLostException(string? message = null, Exception? innerException = null)
    : Exception(message ?? "Транзакционный продюсер отклонён брокером (fencing).", innerException);
