using CdcOutbox.Core.Partitions;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class HourlyPartitionNameTests
{
    [Fact]
    public void TruncateToHourUtc_ConvertsOffsetToUtcHourFloor()
    {
        // 2026-11-24 20:42:15 +03:00 == 2026-11-24 17:42:15 UTC
        var moscow = new DateTimeOffset(2026, 11, 24, 20, 42, 15, TimeSpan.FromHours(3));

        var truncated = HourlyPartitionName.TruncateToHourUtc(moscow);

        truncated.ShouldBe(new DateTimeOffset(2026, 11, 24, 17, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void BuildHorizon_48HoursAhead_Produces49ContiguousHourlyWindowsAcrossYearRollover()
    {
        var now = new DateTimeOffset(2026, 12, 31, 22, 35, 10, TimeSpan.Zero);

        var horizon = HourlyPartitionName.BuildHorizon("Messages_Partitioned", now, precreateHours: 48);

        horizon.Count.ShouldBe(49);
        horizon[0].PartitionName.ShouldBe("messages_partitioned_20261231_22");
        horizon[0].RangeStartUtc.ShouldBe(new DateTimeOffset(2026, 12, 31, 22, 0, 0, TimeSpan.Zero));
        horizon[0].RangeEndUtc.ShouldBe(new DateTimeOffset(2026, 12, 31, 23, 0, 0, TimeSpan.Zero));

        horizon[2].PartitionName.ShouldBe("messages_partitioned_20270101_00");
        horizon[^1].PartitionName.ShouldBe("messages_partitioned_20270102_22");
        horizon[^1].RangeEndUtc.ShouldBe(new DateTimeOffset(2027, 1, 2, 23, 0, 0, TimeSpan.Zero));

        for (var i = 1; i < horizon.Count; i++)
        {
            horizon[i].RangeStartUtc.ShouldBe(horizon[i - 1].RangeEndUtc);
            (horizon[i].RangeEndUtc - horizon[i].RangeStartUtc).ShouldBe(TimeSpan.FromHours(1));
        }
    }

    [Theory]
    [InlineData("2026-11-24T17:00:00Z", 5, "2026-11-24T17:05:00Z")]
    [InlineData("2026-11-24T17:04:59Z", 5, "2026-11-24T17:05:00Z")]
    [InlineData("2026-11-24T17:05:00Z", 5, "2026-11-24T18:05:00Z")]
    [InlineData("2026-11-24T17:42:13Z", 5, "2026-11-24T18:05:00Z")]
    [InlineData("2026-11-24T23:30:00Z", 5, "2026-11-25T00:05:00Z")]
    public void CalculateNextRunUtc_AlignedToHH05_WithoutDrift(string nowIso, int minute, string expectedIso)
    {
        var now = DateTimeOffset.Parse(nowIso);
        var expected = DateTimeOffset.Parse(expectedIso);

        var next = HourlyPartitionName.CalculateNextRunUtc(now, minute);

        next.ShouldBe(expected);
    }

    [Fact]
    public void CalculateNextRunUtc_Over72Iterations_NeverDriftsFromMinute05()
    {
        var cursor = new DateTimeOffset(2026, 11, 24, 10, 5, 0, TimeSpan.Zero);
        for (var i = 0; i < 72; i++)
        {
            cursor = HourlyPartitionName.CalculateNextRunUtc(cursor, runAtMinuteOfHour: 5);
            cursor.Minute.ShouldBe(5);
            cursor.Second.ShouldBe(0);
        }

        cursor.ShouldBe(new DateTimeOffset(2026, 11, 27, 10, 5, 0, TimeSpan.Zero));
    }
}
