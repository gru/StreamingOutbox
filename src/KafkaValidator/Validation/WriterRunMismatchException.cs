namespace KafkaValidator.Validation;

/// <summary>
/// Последний прогон Writer — другой runId — 409 (набор чужого прогона нельзя молча подставлять).
/// </summary>
public sealed class WriterRunMismatchException(string message) : Exception(message);
