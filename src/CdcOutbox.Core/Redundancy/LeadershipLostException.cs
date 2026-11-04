namespace CdcOutbox.Core.Redundancy;

/// <summary>
/// Сработал fencing продюсера — лидерство потеряно.
/// </summary>
public sealed class LeadershipLostException(string? message = null, Exception? innerException = null)
    : Exception(message ?? "Лидерство потеряно: сработал fencing транзакционного продюсера.", innerException);
