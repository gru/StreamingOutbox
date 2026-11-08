namespace Writer.Generation;

/// <summary>
/// Результат выполнения батча.
/// </summary>
public sealed record BatchResult(
    BatchOutcome Outcome,
    int Attempts,
    double BatchDurationMs,
    double CommitDurationMs,
    string? FatalError = null,
    string? LastTransientError = null);
