using System.Globalization;
using System.Text;

namespace Writer.Generation;

/// <summary>
/// Сообщение тестовой нагрузки — строка таблицы outbox.messages (dependencies.md §3).
/// body — UTF-8 JSON {"id","runId","eventType","payload","timestamp"}; key — UTF-8 десятичного
/// представления id; headers — "{}". Формат body/key — контракт Writer↔KafkaValidator
/// (outbox-table.md), общий код между сервисами не используется.
/// </summary>
/// <param name="BodyBytes">Размер body в UTF-8 байтах — база учёта объёма прогона.</param>
public sealed record GeneratedMessage(
    long Id,
    string Topic,
    byte[] Key,
    byte[] Body,
    string Headers,
    DateTimeOffset CreatedAt,
    int BodyBytes);

public static class MessageFactory
{
    public const string Topic = "outbox.events";
    public const string EventType = "TestEvent";
    public const string EmptyHeaders = "{}";

    /// <summary>Оценка ширины id (десятичных цифр) для расчёта размера body до генерации.</summary>
    private const int IdDigitsEstimate = 12;

    /// <summary>Длина timestamp в формате "yyyy-MM-ddTHH:mm:ss.fffZ".</summary>
    private const int TimestampLength = 24;

    private const string Alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /// <summary>
    /// id — заранее зарезервированное значение identity-последовательности outbox.messages
    /// (SELECT nextval, см. OutboxWriter.ReserveIdsAsync): идентичность сообщения (= id строки)
    /// обязана попасть в body и key ДО вставки, а стабильность id на ретраях батча даёт
    /// детект AlreadyCommitted по PK-конфликту (разрешение неоднозначного commit).
    /// </summary>
    public static GeneratedMessage Create(long id, Guid runId, int payloadBytes, DateTimeOffset? createdAt = null)
    {
        payloadBytes = Math.Max(0, payloadBytes);
        var payload = new char[payloadBytes];
        for (var i = 0; i < payloadBytes; i++)
            payload[i] = Alphabet[Random.Shared.Next(Alphabet.Length)];

        var createdAtValue = createdAt ?? DateTimeOffset.UtcNow;
        // Псевдослучайный alphanumeric payload: 1 символ ASCII = 1 байт UTF-8, размер
        // контролируется точно; неконстантное содержимое не даёт TOAST-компрессии
        // занижать фактический объём body/WAL (важно для WAL-метрик).
        var body = $"{{\"id\":{id.ToString(CultureInfo.InvariantCulture)},\"runId\":\"{runId}\"," +
                   $"\"eventType\":\"{EventType}\",\"payload\":\"{new string(payload)}\"," +
                   $"\"timestamp\":\"{createdAtValue:yyyy-MM-dd'T'HH:mm:ss.fff'Z'}\"}}";
        return new GeneratedMessage(
            id,
            Topic,
            Encoding.UTF8.GetBytes(id.ToString(CultureInfo.InvariantCulture)),
            Encoding.UTF8.GetBytes(body),
            EmptyHeaders,
            createdAtValue,
            Encoding.UTF8.GetByteCount(body));
    }

    public static (GeneratedMessage[] Messages, long TotalBodyBytes) CreateBatch(long[] ids, Guid runId, int payloadBytes)
    {
        var messages = new GeneratedMessage[ids.Length];
        long bytes = 0;
        for (var i = 0; i < ids.Length; i++)
        {
            messages[i] = Create(ids[i], runId, payloadBytes);
            bytes += messages[i].BodyBytes;
        }
        return (messages, bytes);
    }

    /// <summary>Оценка размера body до генерации (payload фиксирован, runId — 36 символов, ширина id — оценка).</summary>
    public static int EstimateBodyBytes(Guid runId, int payloadBytes) =>
        Encoding.UTF8.GetByteCount(
            $"{{\"id\":{new string('1', IdDigitsEstimate)},\"runId\":\"{runId}\"," +
            $"\"eventType\":\"{EventType}\",\"payload\":\"{new string('x', payloadBytes)}\"," +
            $"\"timestamp\":\"{new string('x', TimestampLength)}\"}}");
}
