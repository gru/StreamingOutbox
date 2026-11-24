using System.Buffers.Binary;
using System.Text;

namespace CdcOutbox.Core.Replication;

/// <summary>
/// Бинарный кодек кадров логических WAL-сообщений (формат OBX1, big-endian).
/// Совместим как с одиночной упаковкой на стороне PL/pgSQL (int4send/int8send),
/// так и с пакетной упаковкой SuperFrame на стороне приложения.
/// </summary>
public static class WalOutboxFrameCodec
{
    private const uint Magic = 0x4F425831; // "OBX1"
    private const byte FlagHasPartition = 1 << 0;
    private const byte FlagHasProducerKey = 1 << 1;
    private const int HeaderBytes = 8;

    /// <summary>
    /// Закодировать одно или несколько сообщений в бинарный кадр OBX1.
    /// </summary>
    /// <param name="messages">Непустой список сообщений.</param>
    /// <returns>Байты кадра для передачи в pg_logical_emit_message.</returns>
    public static byte[] Encode(IReadOnlyList<OutboxMessage> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            throw new ArgumentException("Список сообщений для упаковки в OBX1 не может быть пустым.", nameof(messages));
        }

        var totalSize = HeaderBytes;
        for (var i = 0; i < messages.Count; i++)
        {
            var message = messages[i];
            var topicLen = Encoding.UTF8.GetByteCount(message.Topic);
            var headersLen = Encoding.UTF8.GetByteCount(message.Headers);
            var producerLen = message.ProducerKey is null ? 0 : Encoding.UTF8.GetByteCount(message.ProducerKey);

            totalSize += 8 + 8 + 1;
            if (message.Partition.HasValue)
            {
                totalSize += 4;
            }

            totalSize += 4 + topicLen;
            totalSize += 4 + message.Key.Length;
            totalSize += 4 + message.Body.Length;
            totalSize += 4 + headersLen;
            if (message.ProducerKey is not null)
            {
                totalSize += 4 + producerLen;
            }
        }

        var buffer = new byte[totalSize];
        var span = buffer.AsSpan();
        BinaryPrimitives.WriteUInt32BigEndian(span[..4], Magic);
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(4, 4), messages.Count);
        var offset = HeaderBytes;

        for (var i = 0; i < messages.Count; i++)
        {
            var message = messages[i];
            BinaryPrimitives.WriteInt64BigEndian(span.Slice(offset, 8), message.Id);
            offset += 8;

            var utc = message.CreatedAt.Kind == DateTimeKind.Utc
                ? message.CreatedAt
                : DateTime.SpecifyKind(message.CreatedAt, DateTimeKind.Utc);
            var unixMicros = (utc.Ticks - DateTime.UnixEpoch.Ticks) / 10;
            BinaryPrimitives.WriteInt64BigEndian(span.Slice(offset, 8), unixMicros);
            offset += 8;

            byte flags = 0;
            if (message.Partition.HasValue)
            {
                flags |= FlagHasPartition;
            }

            if (message.ProducerKey is not null)
            {
                flags |= FlagHasProducerKey;
            }

            span[offset++] = flags;

            if (message.Partition is { } partition)
            {
                BinaryPrimitives.WriteInt32BigEndian(span.Slice(offset, 4), partition);
                offset += 4;
            }

            offset = WriteUtf8(span, offset, message.Topic);
            offset = WriteBytes(span, offset, message.Key);
            offset = WriteBytes(span, offset, message.Body);
            offset = WriteUtf8(span, offset, message.Headers);

            if (message.ProducerKey is not null)
            {
                offset = WriteUtf8(span, offset, message.ProducerKey);
            }
        }

        return buffer;
    }

    /// <summary>
    /// Декодировать бинарный кадр OBX1 и добавить сообщения в целевой список.
    /// </summary>
    /// <param name="frame">Содержимое LogicalDecodingMessage.Data.</param>
    /// <param name="destination">Список для добавления декодированных сообщений.</param>
    /// <returns>Количество декодированных сообщений.</returns>
    public static int Decode(ReadOnlySpan<byte> frame, List<OutboxMessage> destination)
    {
        ArgumentNullException.ThrowIfNull(destination);
        if (frame.Length < HeaderBytes)
        {
            throw new FormatException(
                $"Кадр OBX1 слишком короткий: {frame.Length} байт (минимум {HeaderBytes}).");
        }

        var magic = BinaryPrimitives.ReadUInt32BigEndian(frame[..4]);
        if (magic != Magic)
        {
            throw new FormatException(
                $"Неизвестная сигнатура WAL-кадра: 0x{magic:X8} (ожидается OBX1 / 0x{Magic:X8}).");
        }

        var count = BinaryPrimitives.ReadInt32BigEndian(frame.Slice(4, 4));
        if (count <= 0)
        {
            throw new FormatException($"Некорректное число сообщений в кадре OBX1: {count}.");
        }

        var offset = HeaderBytes;
        for (var i = 0; i < count; i++)
        {
            EnsureRemaining(frame, offset, 8 + 8 + 1, "заголовок сообщения");
            var id = BinaryPrimitives.ReadInt64BigEndian(frame.Slice(offset, 8));
            offset += 8;

            var unixMicros = BinaryPrimitives.ReadInt64BigEndian(frame.Slice(offset, 8));
            offset += 8;

            var flags = frame[offset++];

            int? partition = null;
            if ((flags & FlagHasPartition) != 0)
            {
                EnsureRemaining(frame, offset, 4, "partition");
                partition = BinaryPrimitives.ReadInt32BigEndian(frame.Slice(offset, 4));
                offset += 4;
            }

            var topic = ReadUtf8(frame, ref offset, "topic");
            var key = ReadBytes(frame, ref offset, "key");
            var body = ReadBytes(frame, ref offset, "body");
            var headers = ReadUtf8(frame, ref offset, "headers");

            string? producerKey = null;
            if ((flags & FlagHasProducerKey) != 0)
            {
                producerKey = ReadUtf8(frame, ref offset, "producer_key");
            }

            var createdAt = FromUnixMicroseconds(unixMicros);
            destination.Add(new OutboxMessage
            {
                Id = id,
                Topic = topic,
                Key = key,
                Body = body,
                Headers = headers,
                Partition = partition,
                ProducerKey = producerKey,
                CreatedAt = createdAt,
            });
        }

        if (offset != frame.Length)
        {
            throw new FormatException(
                $"Лишние хвостовые байты в кадре OBX1: прочитано {offset}, всего {frame.Length}.");
        }

        return count;
    }

    private static DateTime FromUnixMicroseconds(long unixMicros)
    {
        try
        {
            var ticks = checked(DateTime.UnixEpoch.Ticks + (unixMicros * 10));
            return new DateTime(ticks, DateTimeKind.Utc);
        }
        catch (Exception exception) when (exception is OverflowException or ArgumentOutOfRangeException)
        {
            throw new FormatException($"Метка времени OBX1 вне допустимого диапазона DateTime: {unixMicros} мкс.", exception);
        }
    }

    private static int WriteUtf8(Span<byte> target, int offset, string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteInt32BigEndian(target.Slice(offset, 4), byteCount);
        offset += 4;
        if (byteCount > 0)
        {
            Encoding.UTF8.GetBytes(value, target.Slice(offset, byteCount));
            offset += byteCount;
        }

        return offset;
    }

    private static int WriteBytes(Span<byte> target, int offset, byte[] value)
    {
        BinaryPrimitives.WriteInt32BigEndian(target.Slice(offset, 4), value.Length);
        offset += 4;
        if (value.Length > 0)
        {
            value.CopyTo(target.Slice(offset, value.Length));
            offset += value.Length;
        }

        return offset;
    }

    private static string ReadUtf8(ReadOnlySpan<byte> frame, ref int offset, string fieldName)
    {
        var length = ReadLength(frame, ref offset, fieldName);
        if (length == 0)
        {
            return string.Empty;
        }

        var result = Encoding.UTF8.GetString(frame.Slice(offset, length));
        offset += length;
        return result;
    }

    private static byte[] ReadBytes(ReadOnlySpan<byte> frame, ref int offset, string fieldName)
    {
        var length = ReadLength(frame, ref offset, fieldName);
        if (length == 0)
        {
            return Array.Empty<byte>();
        }

        var result = frame.Slice(offset, length).ToArray();
        offset += length;
        return result;
    }

    private static int ReadLength(ReadOnlySpan<byte> frame, ref int offset, string fieldName)
    {
        EnsureRemaining(frame, offset, 4, fieldName);
        var length = BinaryPrimitives.ReadInt32BigEndian(frame.Slice(offset, 4));
        offset += 4;
        if (length < 0)
        {
            throw new FormatException($"Отрицательная длина поля {fieldName} в кадре OBX1: {length}.");
        }

        EnsureRemaining(frame, offset, length, fieldName);
        return length;
    }

    private static void EnsureRemaining(ReadOnlySpan<byte> frame, int offset, int required, string fieldName)
    {
        if (required < 0 || offset > frame.Length - required)
        {
            throw new FormatException(
                $"Обрыв кадра OBX1 при чтении поля {fieldName}: смещение {offset}, требуется {required}, длина кадра {frame.Length}.");
        }
    }
}
