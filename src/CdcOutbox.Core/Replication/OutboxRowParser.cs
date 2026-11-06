using System.Buffers.Binary;
using System.Globalization;
using System.Text;

namespace CdcOutbox.Core.Replication;

internal enum OutboxColumnSlot
{
    Id,
    Topic,
    Key,
    Body,
    Headers,
    Partition,
    ProducerKey,
    CreatedAt,
    Ignored,
}

internal static class OutboxRowParser
{
    private const int RequiredColumns =
        (1 << (int)OutboxColumnSlot.Id)
        | (1 << (int)OutboxColumnSlot.Topic)
        | (1 << (int)OutboxColumnSlot.Key)
        | (1 << (int)OutboxColumnSlot.Body)
        | (1 << (int)OutboxColumnSlot.Headers)
        | (1 << (int)OutboxColumnSlot.CreatedAt);

    private const uint BigIntOid = 20;
    private const uint IntegerOid = 23;
    private const uint ByteaOid = 17;
    private const uint VarcharOid = 1043;
    private const uint TimestampTzOid = 1184;

    private static readonly string[] TimestampFormats =
    {
        "yyyy-MM-dd HH:mm:ss.FFFFFFzzz",
        "yyyy-MM-dd HH:mm:ss.FFFFFF",
    };

    private static readonly DateTime PostgresEpoch = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    // границы DateTime в микросекундах от эпохи PostgreSQL (±1 — запас усечения деления)
    private static readonly long MinTimestampMicros = (DateTime.MinValue.Ticks - PostgresEpoch.Ticks) / 10 - 1;
    private static readonly long MaxTimestampMicros = (DateTime.MaxValue.Ticks - PostgresEpoch.Ticks) / 10 + 1;

    internal static OutboxColumnPlan BuildColumnPlan(IReadOnlyList<string> columnNames, IReadOnlyList<uint> dataTypeIds)
    {
        var slots = new OutboxColumnSlot[columnNames.Count];
        var typeIds = new uint[columnNames.Count];
        var seen = 0;
        for (var i = 0; i < columnNames.Count; i++)
        {
            var slot = columnNames[i] switch
            {
                "id" => OutboxColumnSlot.Id,
                "topic" => OutboxColumnSlot.Topic,
                "key" => OutboxColumnSlot.Key,
                "body" => OutboxColumnSlot.Body,
                "headers" => OutboxColumnSlot.Headers,
                "partition" => OutboxColumnSlot.Partition,
                "producer_key" => OutboxColumnSlot.ProducerKey,
                "created_at" => OutboxColumnSlot.CreatedAt,
                _ => OutboxColumnSlot.Ignored,
            };
            slots[i] = slot;
            typeIds[i] = dataTypeIds[i];
            seen |= 1 << (int)slot;
        }

        if ((seen & RequiredColumns) != RequiredColumns)
        {
            var missing = new List<string>();
            AppendMissing(missing, seen, OutboxColumnSlot.Id, "id");
            AppendMissing(missing, seen, OutboxColumnSlot.Topic, "topic");
            AppendMissing(missing, seen, OutboxColumnSlot.Key, "key");
            AppendMissing(missing, seen, OutboxColumnSlot.Body, "body");
            AppendMissing(missing, seen, OutboxColumnSlot.Headers, "headers");
            AppendMissing(missing, seen, OutboxColumnSlot.CreatedAt, "created_at");
            throw new InvalidOperationException(
                $"Таблица outbox не соответствует ожидаемой структуре: отсутствуют колонки {string.Join(", ", missing)}.");
        }

        return new OutboxColumnPlan(slots, typeIds);
    }

    /// <summary>
    /// Проверка OID колонок против бинарного контракта (outbox-table.md «Бинарный формат значений»):
    /// несовпадение — ошибка декодирования с именем колонки, ещё до чтения значений.
    /// </summary>
    internal static void EnsureBinaryTypes(OutboxColumnPlan plan, IReadOnlyList<string> columnNames)
    {
        for (var i = 0; i < plan.Slots.Length; i++)
        {
            var slot = plan.Slots[i];
            if (slot == OutboxColumnSlot.Ignored)
            {
                continue;
            }

            var expected = BinaryTypeId(slot);
            if (plan.TypeIds[i] != expected)
            {
                throw new FormatException(
                    $"Колонка {columnNames[i]}: тип OID {plan.TypeIds[i]} не соответствует бинарному контракту (ожидается OID {expected}).");
            }
        }
    }

    internal static OutboxMessage Parse(
        string? id,
        string? topic,
        string? key,
        string? body,
        string? headers,
        string? partition,
        string? producerKey,
        string? createdAt)
    {
        id = ThrowIfNull(id, "id");

        if (!long.TryParse(id, NumberStyles.Integer, CultureInfo.InvariantCulture, out var messageId))
        {
            throw new FormatException($"Колонка id: значение '{id}' не разбирается как bigint.");
        }

        topic = ThrowIfNull(topic, "topic");
        key = ThrowIfNull(key, "key");
        body = ThrowIfNull(body, "body");

        var keyBytes = ParseHex(key, "key");
        var bodyBytes = ParseHex(body, "body");

        headers = ThrowIfNull(headers, "headers");

        int? partitionNumber = null;
        if (partition is not null)
        {
            if (!int.TryParse(partition, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedPartition))
            {
                throw new FormatException($"Колонка partition: значение '{partition}' не разбирается как integer.");
            }

            partitionNumber = parsedPartition;
        }

        createdAt = ThrowIfNull(createdAt, "created_at");

        var timestamp = ParseTimestamp(createdAt);

        return new OutboxMessage
        {
            Id = messageId,
            Topic = topic,
            Key = keyBytes,
            Body = bodyBytes,
            Headers = headers,
            Partition = partitionNumber,
            ProducerKey = producerKey,
            CreatedAt = timestamp,
        };
    }

    /// <summary>
    /// Декодирование строки из бинарных значений колонок (бинарный контракт —
    /// outbox-table.md «Бинарный формат значений»): массив байтов — сырые значения
    /// pgoutput, NULL передан как null.
    /// </summary>
    internal static OutboxMessage ParseBinary(
        byte[]? id,
        byte[]? topic,
        byte[]? key,
        byte[]? body,
        byte[]? headers,
        byte[]? partition,
        byte[]? producerKey,
        byte[]? createdAt)
    {
        id = ThrowIfNull(id, "id");

        var messageId = ParseBinaryInt64(id, "id");

        topic = ThrowIfNull(topic, "topic");
        key = ThrowIfNull(key, "key");
        body = ThrowIfNull(body, "body");
        headers = ThrowIfNull(headers, "headers");

        int? partitionNumber = partition is null ? null : ParseBinaryInt32(partition, "partition");

        createdAt = ThrowIfNull(createdAt, "created_at");

        var timestamp = ParseBinaryTimestamp(createdAt, "created_at");

        return new OutboxMessage
        {
            Id = messageId,
            Topic = ParseBinaryText(topic),
            Key = key,
            Body = body,
            Headers = ParseBinaryText(headers),
            Partition = partitionNumber,
            ProducerKey = producerKey is null ? null : ParseBinaryText(producerKey),
            CreatedAt = timestamp,
        };
    }

    internal static long ParseBinaryInt64(ReadOnlySpan<byte> value, string columnName)
    {
        if (value.Length != 8)
        {
            throw new FormatException($"Колонка {columnName}: бинарное значение bigint имеет длину {value.Length}, ожидается 8.");
        }

        return BinaryPrimitives.ReadInt64BigEndian(value);
    }

    internal static int ParseBinaryInt32(ReadOnlySpan<byte> value, string columnName)
    {
        if (value.Length != 4)
        {
            throw new FormatException($"Колонка {columnName}: бинарное значение integer имеет длину {value.Length}, ожидается 4.");
        }

        return BinaryPrimitives.ReadInt32BigEndian(value);
    }

    internal static string ParseBinaryText(ReadOnlySpan<byte> value) => Encoding.UTF8.GetString(value);

    internal static DateTime ParseBinaryTimestamp(ReadOnlySpan<byte> value, string columnName)
    {
        if (value.Length != 8)
        {
            throw new FormatException($"Колонка {columnName}: бинарное значение timestamptz имеет длину {value.Length}, ожидается 8.");
        }

        var micros = BinaryPrimitives.ReadInt64BigEndian(value);
        if (micros < MinTimestampMicros || micros > MaxTimestampMicros)
        {
            throw new FormatException($"Колонка {columnName}: бинарное значение timestamptz ({micros} мкс от 2000-01-01 UTC) вне диапазона DateTime.");
        }

        return PostgresEpoch.AddTicks(micros * 10);
    }

    internal static byte[] ParseHex(string value, string columnName)
    {
        if (!value.AsSpan().StartsWith("\\x"))
        {
            throw new FormatException($"Колонка {columnName}: значение bytea не в hex-формате PostgreSQL (префикс \\x): '{value}'.");
        }

        try
        {
            return Convert.FromHexString(value.AsSpan(2));
        }
        catch (FormatException ex)
        {
            throw new FormatException($"Колонка {columnName}: значение не является корректным hex: '{value}'.", ex);
        }
    }

    internal static DateTime ParseTimestamp(string value)
    {
        if (DateTimeOffset.TryParseExact(
                value.AsSpan(),
                TimestampFormats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var exact))
        {
            return exact.UtcDateTime;
        }

        if (DateTimeOffset.TryParse(
                value.AsSpan(),
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var general))
        {
            return general.UtcDateTime;
        }

        throw new FormatException($"Колонка created_at: значение '{value}' не разбирается как timestamptz.");
    }

    private static uint BinaryTypeId(OutboxColumnSlot slot) => slot switch
    {
        OutboxColumnSlot.Id => BigIntOid,
        OutboxColumnSlot.Topic or OutboxColumnSlot.Headers or OutboxColumnSlot.ProducerKey => VarcharOid,
        OutboxColumnSlot.Key or OutboxColumnSlot.Body => ByteaOid,
        OutboxColumnSlot.Partition => IntegerOid,
        OutboxColumnSlot.CreatedAt => TimestampTzOid,
        _ => 0,
    };

    private static void AppendMissing(List<string> missing, int seen, OutboxColumnSlot slot, string columnName)
    {
        if ((seen & (1 << (int)slot)) == 0)
        {
            missing.Add(columnName);
        }
    }

    private static T ThrowIfNull<T>(T? value, string columnName) where T : notnull =>
        value ?? throw new FormatException($"Колонка {columnName}: NULL в NOT NULL колонке.");
}
