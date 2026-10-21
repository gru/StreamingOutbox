namespace KafkaValidator.Validation;

/// <summary>
/// Writer недоступен или не может отдать набор (нет завершённого прогона) — 502.
/// </summary>
public sealed class WriterUnavailableException(string message) : Exception(message);
