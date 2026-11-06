namespace Writer.Generation;

/// <summary>
/// Сообщение тестовой нагрузки — строка таблицы outbox.messages.
/// </summary>
public sealed record GeneratedMessage(
    long Id,
    string Topic,
    byte[] Key,
    byte[] Body,
    string Headers,
    DateTimeOffset CreatedAt,
    int BodyBytes);
