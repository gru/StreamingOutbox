using System.Text;
using CdcOutbox.Core.Kafka;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class CheckpointStoreCodecTests
{
    [Fact]
    public void Value_RoundTrips()
    {
        var record = new CheckpointStore.CheckpointRecord(
            "outbox_slot",
            "0/1AB",
            new DateTime(2026, 11, 1, 7, 30, 0, DateTimeKind.Utc));

        CheckpointStore.DecodeValue(CheckpointStore.EncodeValue(record), offset: 7).ShouldBe(0x1ABUL);
    }

    [Fact]
    public void BrokenJson_FailsWithOffsetInMessage()
    {
        var error = Should.Throw<InvalidOperationException>(
            () => CheckpointStore.DecodeValue(Encoding.UTF8.GetBytes("не json"), offset: 42));

        error.Message.ShouldContain("42");
    }

    [Fact]
    public void MissingLsn_Fails()
    {
        var error = Should.Throw<InvalidOperationException>(
            () => CheckpointStore.DecodeValue(Encoding.UTF8.GetBytes("{\"slotName\":\"outbox_slot\"}"), offset: 1));

        error.Message.ShouldContain("не содержит корректный LSN");
    }

    [Fact]
    public void MalformedLsn_Fails()
    {
        var error = Should.Throw<InvalidOperationException>(
            () => CheckpointStore.DecodeValue(Encoding.UTF8.GetBytes("{\"slotName\":\"outbox_slot\",\"lsn\":\"zzz\"}"), offset: 1));

        error.Message.ShouldContain("не содержит корректный LSN");
    }
}
