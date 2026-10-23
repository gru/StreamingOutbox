using CdcOutbox.Core.Replication;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class OutboxRowParserTests
{
    [Fact]
    public void ParseHex_ValidPgHex_DecodesBytes()
    {
        OutboxRowParser.ParseHex("\\x48656c6c6f", "key").ShouldBe("Hello"u8.ToArray());
    }

    [Fact]
    public void ParseHex_MissingPrefix_FailsWithColumnName()
    {
        var error = Should.Throw<FormatException>(() => OutboxRowParser.ParseHex("48656c6c6f", "body"));

        error.Message.ShouldContain("body");
        error.Message.ShouldContain("hex");
    }

    [Fact]
    public void ParseHex_GarbageAfterPrefix_FailsWithColumnName()
    {
        var error = Should.Throw<FormatException>(() => OutboxRowParser.ParseHex("\\xzz", "key"));

        error.Message.ShouldContain("key");
    }

    [Fact]
    public void ParseTimestamp_MicrosecondsWithUtcOffset_ParsesAsUtc()
    {
        var timestamp = OutboxRowParser.ParseTimestamp("2026-10-17 06:42:11.123456+00");

        timestamp.ShouldBe(new DateTime(2026, 10, 17, 6, 42, 11, 123, 456, DateTimeKind.Utc));
        timestamp.Kind.ShouldBe(DateTimeKind.Utc);
    }

    [Fact]
    public void ParseTimestamp_ZoneWithoutMinutes_ConvertsToUtc()
    {
        var timestamp = OutboxRowParser.ParseTimestamp("2026-10-17 06:42:11+03");

        timestamp.ShouldBe(new DateTime(2026, 10, 17, 3, 42, 11, DateTimeKind.Utc));
    }

    [Fact]
    public void ParseTimestamp_NoFraction_ParsesAsUtc()
    {
        var timestamp = OutboxRowParser.ParseTimestamp("2026-10-17 06:42:11+00");

        timestamp.ShouldBe(new DateTime(2026, 10, 17, 6, 42, 11, DateTimeKind.Utc));
    }

    [Fact]
    public void ParseTimestamp_Garbage_Fails()
    {
        Should.Throw<FormatException>(() => OutboxRowParser.ParseTimestamp("not-a-timestamp"));
    }

    [Fact]
    public void Parse_FullRow_MapsAllFields()
    {
        var message = OutboxRowParser.Parse(
            "42",
            "orders.events",
            "\\x0123456789abcdef",
            "\\xdeadbeef",
            "{}",
            "7",
            "cluster-b",
            "2026-10-17 06:42:11.123456+00");

        message.Id.ShouldBe(42);
        message.Topic.ShouldBe("orders.events");
        message.Key.ShouldBe(new byte[] { 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef });
        message.Body.ShouldBe(new byte[] { 0xde, 0xad, 0xbe, 0xef });
        message.Headers.ShouldBe("{}");
        message.Partition.ShouldBe(7);
        message.ProducerKey.ShouldBe("cluster-b");
        message.CreatedAt.ShouldBe(new DateTime(2026, 10, 17, 6, 42, 11, 123, 456, DateTimeKind.Utc));
    }

    [Fact]
    public void Parse_NullOptionalColumns_KeepsNulls()
    {
        var message = OutboxRowParser.Parse(
            "1",
            "orders.events",
            "\\x00",
            "\\x01",
            "{}",
            null,
            null,
            "2026-10-17 06:42:11+00");

        message.Partition.ShouldBeNull();
        message.ProducerKey.ShouldBeNull();
    }

    [Fact]
    public void Parse_NullInRequiredColumn_FailsWithColumnName()
    {
        var error = Should.Throw<FormatException>(() =>
            OutboxRowParser.Parse("1", null, "\\x00", "\\x01", "{}", null, null, "2026-10-17 06:42:11+00"));

        error.Message.ShouldContain("topic");
    }

    [Fact]
    public void Parse_InvalidId_FailsWithColumnName()
    {
        var error = Should.Throw<FormatException>(() =>
            OutboxRowParser.Parse("abc", "t", "\\x00", "\\x01", "{}", null, null, "2026-10-17 06:42:11+00"));

        error.Message.ShouldContain("id");
    }

    [Fact]
    public void Parse_InvalidPartition_FailsWithColumnName()
    {
        var error = Should.Throw<FormatException>(() =>
            OutboxRowParser.Parse("1", "t", "\\x00", "\\x01", "{}", "x", null, "2026-10-17 06:42:11+00"));

        error.Message.ShouldContain("partition");
    }

    [Fact]
    public void BuildColumnPlan_FullContract_MapsSlotsAndIgnoresUnknown()
    {
        var plan = OutboxRowParser.BuildColumnPlan(new[]
        {
            "id", "topic", "key", "body", "headers", "partition", "producer_key", "created_at", "outbox_partition",
        });

        plan.Slots.ShouldBe(new[]
        {
            OutboxColumnSlot.Id,
            OutboxColumnSlot.Topic,
            OutboxColumnSlot.Key,
            OutboxColumnSlot.Body,
            OutboxColumnSlot.Headers,
            OutboxColumnSlot.Partition,
            OutboxColumnSlot.ProducerKey,
            OutboxColumnSlot.CreatedAt,
            OutboxColumnSlot.Ignored,
        });
    }

    [Fact]
    public void BuildColumnPlan_MissingRequiredColumn_FailsNamingColumn()
    {
        var error = Should.Throw<InvalidOperationException>(() =>
            OutboxRowParser.BuildColumnPlan(new[] { "id", "key", "body", "headers", "created_at" }));

        error.Message.ShouldContain("topic");
        error.Message.ShouldContain("отсутствуют колонки");
    }

    [Fact]
    public void Parse_MinimalAllocations_DoNotBuildCollectionsPerRow()
    {
        const int iterations = 200;
        OutboxRowParser.Parse(
            "42",
            "orders.events",
            "\\x0123456789abcdef",
            "\\x00112233445566778899aabbccddeeff",
            "{}",
            "7",
            "cluster-b",
            "2026-10-17 06:42:11.123456+00");

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
        {
            OutboxRowParser.Parse(
                "42",
                "orders.events",
                "\\x0123456789abcdef",
                "\\x00112233445566778899aabbccddeeff",
                "{}",
                "7",
                "cluster-b",
                "2026-10-17 06:42:11.123456+00");
        }

        var allocatedPerIteration = (GC.GetAllocatedBytesForCurrentThread() - before) / iterations;
        allocatedPerIteration.ShouldBeLessThan(400);
    }
}
