using System.Buffers.Binary;
using System.Text;
using Shouldly;
using Writer.Configuration;
using Writer.Contracts;
using Writer.Generation;
using Xunit;

namespace Writer.UnitTests;

public sealed class WalFrameEncoderTests
{
    [Fact]
    public void Encode_Batch_ProducesValidObx1BigEndianFrame()
    {
        var (messages, _) = MessageFactory.CreateBatch(new long[] { 101, 102, 103 }, Guid.NewGuid(), payloadBytes: 64);

        var frame = WalFrameEncoder.Encode(messages);

        Encoding.ASCII.GetString(frame, 0, 4).ShouldBe("OBX1");
        BinaryPrimitives.ReadInt32BigEndian(frame.AsSpan(4, 4)).ShouldBe(3);
        BinaryPrimitives.ReadInt64BigEndian(frame.AsSpan(8, 8)).ShouldBe(101);
        frame.Length.ShouldBeGreaterThan(200);
    }

    [Fact]
    public void LoadProfileResolve_OverridesWriteModeAndTableName()
    {
        var options = new WriterOptions
        {
            Defaults = new LoadProfileDefaults
            {
                MessageSizeBytes = 256,
                Parallelism = 4,
                BatchSize = 50,
                WriteMode = WriterWriteMode.InsertDelete,
                TableName = "outbox.messages",
            },
        };

        var errors = new List<string>();
        var walProfile = LoadProfile.Resolve(
            options,
            new RunSpec { TotalMessages = 100, WriteMode = WriterWriteMode.WalEmit },
            errors);
        walProfile.ShouldNotBeNull();
        walProfile!.WriteMode.ShouldBe(WriterWriteMode.WalEmit);
        errors.ShouldBeEmpty();

        var partProfile = LoadProfile.Resolve(
            options,
            new RunSpec
            {
                TotalMessages = 100,
                WriteMode = WriterWriteMode.InsertOnly,
                TableName = "outbox.messages_partitioned",
            },
            errors);
        partProfile.ShouldNotBeNull();
        partProfile!.WriteMode.ShouldBe(WriterWriteMode.InsertOnly);
        partProfile.TableName.ShouldBe("outbox.messages_partitioned");
    }
}
