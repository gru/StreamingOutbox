using System.Globalization;

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

internal sealed class OutboxColumnPlan
{
    internal OutboxColumnPlan(OutboxColumnSlot[] slots)
    {
        Slots = slots;
    }

    internal OutboxColumnSlot[] Slots { get; }
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

    private static readonly string[] TimestampFormats =
    {
        "yyyy-MM-dd HH:mm:ss.FFFFFFzzz",
        "yyyy-MM-dd HH:mm:ss.FFFFFF",
    };

    internal static OutboxColumnPlan BuildColumnPlan(IReadOnlyList<string> columnNames)
    {
        var slots = new OutboxColumnSlot[columnNames.Count];
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

        return new OutboxColumnPlan(slots);
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
        if (id is null)
        {
            throw new FormatException("Колонка id: NULL в NOT NULL колонке.");
        }

        if (!long.TryParse(id, NumberStyles.Integer, CultureInfo.InvariantCulture, out var messageId))
        {
            throw new FormatException($"Колонка id: значение '{id}' не разбирается как bigint.");
        }

        if (topic is null)
        {
            throw new FormatException("Колонка topic: NULL в NOT NULL колонке.");
        }

        if (key is null)
        {
            throw new FormatException("Колонка key: NULL в NOT NULL колонке.");
        }

        if (body is null)
        {
            throw new FormatException("Колонка body: NULL в NOT NULL колонке.");
        }

        var keyBytes = ParseHex(key, "key");
        var bodyBytes = ParseHex(body, "body");

        if (headers is null)
        {
            throw new FormatException("Колонка headers: NULL в NOT NULL колонке.");
        }

        int? partitionNumber = null;
        if (partition is not null)
        {
            if (!int.TryParse(partition, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedPartition))
            {
                throw new FormatException($"Колонка partition: значение '{partition}' не разбирается как integer.");
            }

            partitionNumber = parsedPartition;
        }

        if (createdAt is null)
        {
            throw new FormatException("Колонка created_at: NULL в NOT NULL колонке.");
        }

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

    private static void AppendMissing(List<string> missing, int seen, OutboxColumnSlot slot, string columnName)
    {
        if ((seen & (1 << (int)slot)) == 0)
        {
            missing.Add(columnName);
        }
    }
}
