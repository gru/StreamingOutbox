namespace CdcOutbox.Core;

/// <summary>
/// Outbox-сообщение.
/// </summary>
public class OutboxMessage
{
    /// <summary>
    /// Идентификатор сообщения.
    /// </summary>
    public long Id { get; set; }

    /// <summary>
    /// Название топика, в котором будет опубликовано сообщение.
    /// </summary>
    public string Topic { get; set; } = null!;

    /// <summary>
    /// Ключ сообщения.
    /// </summary>
    public byte[] Key { get; set; } = null!;

    /// <summary>
    /// Тело сообщения.
    /// </summary>
    public byte[] Body { get; set; } = null!;

    /// <summary>
    /// Заголовки сообщения.
    /// </summary>
    public string Headers { get; set; } = null!;

    /// <summary>
    /// Номер партиции, в которой будет опубликовано сообщение.
    /// </summary>
    public int? Partition { get; set; }

    /// <summary>
    /// Ключ producer-а.
    /// На основе значения данного свойства определяется
    /// producer (кластер) для публикации сообщения,
    /// сохраненного в outbox.
    /// </summary>
    public string? ProducerKey { get; set; }

    /// <summary>
    /// Дата и время сохранения сообщения в outbox.
    /// </summary>
    public DateTime CreatedAt { get; internal set; }
}
