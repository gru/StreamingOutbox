using System.Buffers.Binary;
using System.Text;
using CdcOutbox.Core;
using CdcOutbox.Core.Replication;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class WalOutboxFrameCodecTests
{
    [Fact]
    public void EncodeDecode_SingleMessageWithAllFields_PreservesEveryField()
    {
        var createdAt = new DateTime(2026, 11, 24, 17, 15, 42, 123, DateTimeKind.Utc).AddTicks(4560);
        var original = new OutboxMessage
        {
            Id = 42_000_000_123L,
            Topic = "bank.payments.events",
            Key = Encoding.UTF8.GetBytes("order-99"),
            Body = Encoding.UTF8.GetBytes("{\"amount\":1500,\"currency\":\"RUB\",\"comment\":\"перевод\"}"),
            Headers = "{\"trace-id\":\"abc-123\",\"source\":\"ядро\"}",
            Partition = 3,
            ProducerKey = "cluster-retail",
            CreatedAt = createdAt,
        };

        var encoded = WalOutboxFrameCodec.Encode(new[] { original });

        // Проверяем точную big-endian раскладку OBX1 (совместимую с int4send/int8send в PostgreSQL)
        Encoding.ASCII.GetString(encoded, 0, 4).ShouldBe("OBX1");
        BinaryPrimitives.ReadInt32BigEndian(encoded.AsSpan(4, 4)).ShouldBe(1);
        BinaryPrimitives.ReadInt64BigEndian(encoded.AsSpan(8, 8)).ShouldBe(original.Id);

        var decoded = new List<OutboxMessage>();
        var count = WalOutboxFrameCodec.Decode(encoded, decoded);

        count.ShouldBe(1);
        decoded.Count.ShouldBe(1);
        var actual = decoded[0];
        actual.Id.ShouldBe(original.Id);
        actual.Topic.ShouldBe(original.Topic);
        actual.Key.ShouldBe(original.Key);
        actual.Body.ShouldBe(original.Body);
        actual.Headers.ShouldBe(original.Headers);
        actual.Partition.ShouldBe(3);
        actual.ProducerKey.ShouldBe("cluster-retail");
        actual.CreatedAt.ShouldBe(createdAt);
        actual.CreatedAt.Kind.ShouldBe(DateTimeKind.Utc);
    }

    [Fact]
    public void EncodeDecode_SuperFrameBatch_PreservesOrderAndNullableFields()
    {
        var baseTime = new DateTime(2026, 11, 24, 10, 0, 0, DateTimeKind.Utc);
        var messages = new List<OutboxMessage>(200);
        for (var i = 0; i < 200; i++)
        {
            messages.Add(new OutboxMessage
            {
                Id = 1000 + i,
                Topic = "outbox.events",
                Key = i % 3 == 0 ? Array.Empty<byte>() : BitConverter.GetBytes(i),
                Body = Encoding.UTF8.GetBytes($"{{\"seq\":{i}}}"),
                Headers = "{}",
                Partition = i % 2 == 0 ? i % 6 : null,
                ProducerKey = i % 5 == 0 ? $"p-{i}" : null,
                CreatedAt = baseTime.AddMilliseconds(i * 10),
            });
        }

        var encoded = WalOutboxFrameCodec.Encode(messages);
        var decoded = new List<OutboxMessage>();
        var count = WalOutboxFrameCodec.Decode(encoded, decoded);

        count.ShouldBe(200);
        decoded.Count.ShouldBe(200);
        for (var i = 0; i < 200; i++)
        {
            decoded[i].Id.ShouldBe(messages[i].Id);
            decoded[i].Key.ShouldBe(messages[i].Key);
            decoded[i].Body.ShouldBe(messages[i].Body);
            decoded[i].Partition.ShouldBe(messages[i].Partition);
            decoded[i].ProducerKey.ShouldBe(messages[i].ProducerKey);
            decoded[i].CreatedAt.ShouldBe(messages[i].CreatedAt);
        }
    }

    [Fact]
    public void Decode_MalformedFrames_ThrowsFormatException()
    {
        Should.Throw<FormatException>(() => WalOutboxFrameCodec.Decode(new byte[] { 1, 2, 3 }, new List<OutboxMessage>()));

        var wrongMagic = Encoding.ASCII.GetBytes("NOPE\x00\x00\x00\x01");
        Should.Throw<FormatException>(() => WalOutboxFrameCodec.Decode(wrongMagic, new List<OutboxMessage>()));

        var zeroCount = new byte[] { (byte)'O', (byte)'B', (byte)'X', (byte)'1', 0, 0, 0, 0 };
        Should.Throw<FormatException>(() => WalOutboxFrameCodec.Decode(zeroCount, new List<OutboxMessage>()));

        var valid = WalOutboxFrameCodec.Encode(new[]
        {
            new OutboxMessage
            {
                Id = 1,
                Topic = "t",
                Key = Array.Empty<byte>(),
                Body = new byte[] { 1 },
                Headers = "{}",
                CreatedAt = DateTime.UtcNow,
            },
        });

        // обрезанный кадр
        Should.Throw<FormatException>(() => WalOutboxFrameCodec.Decode(valid.AsSpan(0, valid.Length - 1), new List<OutboxMessage>()));

        // лишние хвостовые байты
        var withTrailing = valid.Concat(new byte[] { 0xFF }).ToArray();
        Should.Throw<FormatException>(() => WalOutboxFrameCodec.Decode(withTrailing, new List<OutboxMessage>()));
    }
}
