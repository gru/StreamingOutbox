using System.Buffers.Binary;
using CdcOutbox.Core.Replication;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class OutboxRowParserBinaryTests
{
    [Fact]
    public void ParseBinary_FullRow_MapsAllFields()
    {
        var message = OutboxRowParser.ParseBinary(
            Int64Bytes(42),
            "orders.events"u8.ToArray(),
            new byte[] { 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef },
            new byte[] { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff },
            "{}"u8.ToArray(),
            Int32Bytes(7),
            "cluster-b"u8.ToArray(),
            TimestampBytes(CreatedAt));

        message.Id.ShouldBe(42);
        message.Topic.ShouldBe("orders.events");
        message.Key.ShouldBe(new byte[] { 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef });
        message.Body.ShouldBe(new byte[] { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff });
        message.Headers.ShouldBe("{}");
        message.Partition.ShouldBe(7);
        message.ProducerKey.ShouldBe("cluster-b");
        message.CreatedAt.ShouldBe(CreatedAt);
    }

    [Fact]
    public void ParseBinary_NullOptionalColumns_KeepsNulls()
    {
        var message = OutboxRowParser.ParseBinary(
            Int64Bytes(1),
            "orders.events"u8.ToArray(),
            [0x01],
            [0x02],
            "{}"u8.ToArray(),
            null,
            null,
            TimestampBytes(CreatedAt));

        message.Partition.ShouldBeNull();
        message.ProducerKey.ShouldBeNull();
    }

    [Fact]
    public void ParseBinary_NullRequiredColumn_FailsWithColumnName()
    {
        var error = Should.Throw<FormatException>(() =>
            OutboxRowParser.ParseBinary(
                null,
                "orders.events"u8.ToArray(),
                [0x01],
                [0x02],
                "{}"u8.ToArray(),
                null,
                null,
                TimestampBytes(CreatedAt)));

        error.Message.ShouldContain("Колонка id");
        error.Message.ShouldContain("NULL");
    }

    [Fact]
    public void ParseBinary_GarbageLengthInt64_FailsWithColumnName()
    {
        var error = Should.Throw<FormatException>(() =>
            OutboxRowParser.ParseBinary(
                new byte[] { 1, 2, 3 },
                "orders.events"u8.ToArray(),
                [0x01],
                [0x02],
                "{}"u8.ToArray(),
                null,
                null,
                TimestampBytes(CreatedAt)));

        error.Message.ShouldContain("Колонка id");
        error.Message.ShouldContain("длину 3, ожидается 8");
    }

    [Fact]
    public void ParseBinary_GarbageLengthInt32_FailsWithColumnName()
    {
        var error = Should.Throw<FormatException>(() =>
            OutboxRowParser.ParseBinary(
                Int64Bytes(1),
                "orders.events"u8.ToArray(),
                [0x01],
                [0x02],
                "{}"u8.ToArray(),
                new byte[] { 0, 0, 7 },
                null,
                TimestampBytes(CreatedAt)));

        error.Message.ShouldContain("Колонка partition");
        error.Message.ShouldContain("длину 3, ожидается 4");
    }

    [Fact]
    public void ParseBinary_GarbageLengthTimestamp_FailsWithColumnName()
    {
        var error = Should.Throw<FormatException>(() =>
            OutboxRowParser.ParseBinary(
                Int64Bytes(1),
                "orders.events"u8.ToArray(),
                [0x01],
                [0x02],
                "{}"u8.ToArray(),
                null,
                null,
                new byte[9]));

        error.Message.ShouldContain("Колонка created_at");
        error.Message.ShouldContain("длину 9, ожидается 8");
    }

    [Fact]
    public void ParseBinary_TimestampBeforeEpoch_DecodesNegativeMicros()
    {
        // микросекунда до эпохи PostgreSQL: -1 мкс от 2000-01-01 00:00:00 UTC
        var beforeEpoch = OutboxRowParser.ParseBinaryTimestamp(Int64Bytes(-1), "created_at");
        beforeEpoch.ShouldBe(new DateTime(1999, 12, 31, 23, 59, 59, 999, 999, DateTimeKind.Utc));

        var earlier = OutboxRowParser.ParseBinaryTimestamp(
            TimestampBytes(new DateTime(1990, 6, 15, 12, 0, 0, DateTimeKind.Utc)),
            "created_at");
        earlier.ShouldBe(new DateTime(1990, 6, 15, 12, 0, 0, DateTimeKind.Utc));
    }

    [Fact]
    public void ParseBinary_EmptyBytea_StaysEmpty()
    {
        var message = OutboxRowParser.ParseBinary(
            Int64Bytes(1),
            "orders.events"u8.ToArray(),
            [],
            [],
            "{}"u8.ToArray(),
            null,
            null,
            TimestampBytes(CreatedAt));

        message.Key.ShouldBeEmpty();
        message.Body.ShouldBeEmpty();
    }

    [Fact]
    public void ParseBinaryInt64_DecodesBigEndian()
    {
        OutboxRowParser.ParseBinaryInt64(new byte[] { 0, 0, 0, 0, 0, 0, 0, 42 }, "id").ShouldBe(42);
        OutboxRowParser.ParseBinaryInt64(Int64Bytes(long.MinValue), "id").ShouldBe(long.MinValue);
        OutboxRowParser.ParseBinaryInt64(Int64Bytes(-42), "id").ShouldBe(-42);
    }

    [Fact]
    public void ParseBinaryInt32_DecodesBigEndian()
    {
        OutboxRowParser.ParseBinaryInt32(new byte[] { 0, 0, 0, 42 }, "partition").ShouldBe(42);
        OutboxRowParser.ParseBinaryInt32(Int32Bytes(int.MinValue), "partition").ShouldBe(int.MinValue);
        OutboxRowParser.ParseBinaryInt32(Int32Bytes(-42), "partition").ShouldBe(-42);
    }

    [Fact]
    public void ParseBinaryText_DecodesUtf8IncludingCyrillic()
    {
        OutboxRowParser.ParseBinaryText("заказ"u8).ShouldBe("заказ");
        OutboxRowParser.ParseBinaryText([]).ShouldBe(string.Empty);
    }

    [Fact]
    public void ParseBinary_MinimalAllocations_DoNotBuildCollectionsPerRow()
    {
        const int iterations = 200;
        var id = Int64Bytes(42);
        var topic = "orders.events"u8.ToArray();
        var key = new byte[] { 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef };
        var body = new byte[] { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff };
        var headers = "{}"u8.ToArray();
        var partition = Int32Bytes(7);
        var producerKey = "cluster-b"u8.ToArray();
        var createdAt = TimestampBytes(CreatedAt);

        OutboxRowParser.ParseBinary(id, topic, key, body, headers, partition, producerKey, createdAt);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
        {
            OutboxRowParser.ParseBinary(id, topic, key, body, headers, partition, producerKey, createdAt);
        }

        var allocatedPerIteration = (GC.GetAllocatedBytesForCurrentThread() - before) / iterations;
        allocatedPerIteration.ShouldBeLessThan(400);
    }

    [Fact]
    public void EnsureBinaryTypes_TypesMatchContract_Pass()
    {
        var names = new[] { "id", "topic", "key", "body", "headers", "partition", "producer_key", "created_at", "outbox_partition" };
        var plan = OutboxRowParser.BuildColumnPlan(names, new uint[] { 20, 1043, 17, 17, 1043, 23, 1043, 1184, 23 });

        OutboxRowParser.EnsureBinaryTypes(plan, names);
    }

    [Fact]
    public void EnsureBinaryTypes_UnexpectedType_FailsNamingColumn()
    {
        var names = new[] { "id", "topic", "key", "body", "headers", "created_at" };
        var plan = OutboxRowParser.BuildColumnPlan(names, new uint[] { 23, 1043, 17, 17, 1043, 1184 });

        var error = Should.Throw<FormatException>(() => OutboxRowParser.EnsureBinaryTypes(plan, names));

        error.Message.ShouldContain("Колонка id");
        error.Message.ShouldContain("OID 23");
        error.Message.ShouldContain("OID 20");
    }

    [Fact]
    public void EnsureBinaryTypes_IgnoredColumn_AnyTypePasses()
    {
        // игнорируемая колонка (outbox_partition) не входит в контракт — OID не проверяется
        var names = new[] { "id", "topic", "key", "body", "headers", "created_at", "outbox_partition" };
        var plan = OutboxRowParser.BuildColumnPlan(names, new uint[] { 20, 1043, 17, 17, 1043, 1184, 1043 });

        OutboxRowParser.EnsureBinaryTypes(plan, names);
    }

    private static readonly DateTime CreatedAt = new(2026, 10, 17, 6, 42, 11, 123, 456, DateTimeKind.Utc);

    private static readonly long PostgresEpochTicks = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

    private static byte[] Int64Bytes(long value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        return bytes;
    }

    private static byte[] Int32Bytes(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        return bytes;
    }

    private static byte[] TimestampBytes(DateTime utc) => Int64Bytes((utc.Ticks - PostgresEpochTicks) / 10);
}
