using System.Buffers.Binary;
using System.Text;

namespace Writer.Generation;

/// <summary>
/// Упаковщик батча сгенерированных сообщений в бинарный кадр OBX1 (big-endian)
/// для передачи напрямую в WAL через outbox.emit_frame / pg_logical_emit_message.
/// </summary>
public static class WalFrameEncoder
{
    private const uint Magic = 0x4F425831; // "OBX1"
    private const int HeaderBytes = 8;

    /// <summary>
    /// Закодировать непустой батч сообщений в бинарный кадр OBX1.
    /// </summary>
    /// <param name="batch">Сообщения батча.</param>
    /// <returns>Байты кадра OBX1.</returns>
    public static byte[] Encode(GeneratedMessage[] batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Length == 0)
        {
            throw new ArgumentException("Батч для кодирования в OBX1 не может быть пустым.", nameof(batch));
        }

        var totalSize = HeaderBytes;
        for (var i = 0; i < batch.Length; i++)
        {
            var m = batch[i];
            totalSize += 8 + 8 + 1;
            totalSize += 4 + Encoding.UTF8.GetByteCount(m.Topic);
            totalSize += 4 + m.Key.Length;
            totalSize += 4 + m.Body.Length;
            totalSize += 4 + Encoding.UTF8.GetByteCount(m.Headers);
        }

        var buffer = new byte[totalSize];
        var span = buffer.AsSpan();
        BinaryPrimitives.WriteUInt32BigEndian(span[..4], Magic);
        BinaryPrimitives.WriteInt32BigEndian(span.Slice(4, 4), batch.Length);
        var offset = HeaderBytes;

        for (var i = 0; i < batch.Length; i++)
        {
            var m = batch[i];
            BinaryPrimitives.WriteInt64BigEndian(span.Slice(offset, 8), m.Id);
            offset += 8;

            var utcTicks = m.CreatedAt.UtcDateTime.Ticks;
            var unixMicros = (utcTicks - DateTime.UnixEpoch.Ticks) / 10;
            BinaryPrimitives.WriteInt64BigEndian(span.Slice(offset, 8), unixMicros);
            offset += 8;

            // flags = 0 (partition = null, producer_key = null)
            span[offset++] = 0;

            offset = WriteUtf8(span, offset, m.Topic);
            offset = WriteBytes(span, offset, m.Key);
            offset = WriteBytes(span, offset, m.Body);
            offset = WriteUtf8(span, offset, m.Headers);
        }

        return buffer;
    }

    private static int WriteUtf8(Span<byte> target, int offset, string value)
    {
        var count = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteInt32BigEndian(target.Slice(offset, 4), count);
        offset += 4;
        if (count > 0)
        {
            Encoding.UTF8.GetBytes(value, target.Slice(offset, count));
            offset += count;
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
}
