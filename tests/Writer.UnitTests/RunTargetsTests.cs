using Shouldly;
using Writer.Contracts;
using Writer.Generation;
using Xunit;

namespace Writer.UnitTests;

public class RunTargetsTests
{
    private static LoadProfile Profile(
        int? totalMessages = null, long? totalBytes = null, int? durationSec = null,
        bool runUntilStopped = false, int batchSize = 100) => new()
        {
            RunId = Guid.NewGuid(),
            MessageSizeBytes = 1000,
            RatePerSecond = null,
            Parallelism = 4,
            BatchSize = batchSize,
            TotalMessages = totalMessages,
            TotalPayloadBytes = totalBytes,
            DurationSeconds = durationSec,
            RunUntilStopped = runUntilStopped,
        };

    private static readonly DateTimeOffset Start = new(2026, 10, 16, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void StopDispatch_CountTarget()
    {
        var p = Profile(totalMessages: 500);
        RunTargets.StopDispatch(p, 499, 0, Start.AddSeconds(1), Start).ShouldBeFalse();
        RunTargets.StopDispatch(p, 500, 0, Start.AddSeconds(1), Start).ShouldBeTrue();
    }

    [Fact]
    public void StopDispatch_VolumeTarget()
    {
        var p = Profile(totalBytes: 10_000_000);
        RunTargets.StopDispatch(p, 0, 9_999_999, Start.AddSeconds(1), Start).ShouldBeFalse();
        RunTargets.StopDispatch(p, 0, 10_000_000, Start.AddSeconds(1), Start).ShouldBeTrue();
    }

    [Fact]
    public void StopDispatch_DurationTarget()
    {
        var p = Profile(durationSec: 60);
        RunTargets.StopDispatch(p, 1000, 0, Start.AddSeconds(59), Start).ShouldBeFalse();
        RunTargets.StopDispatch(p, 1000, 0, Start.AddSeconds(60), Start).ShouldBeTrue();
    }

    [Fact]
    public void StopDispatch_RunUntilStopped_NeverStopsByItself()
    {
        var p = Profile(runUntilStopped: true);
        RunTargets.StopDispatch(p, 10_000_000, long.MaxValue / 2, Start.AddHours(1), Start).ShouldBeFalse();
    }

    [Fact]
    public void NextBatchSize_ClampedByRemainingMessages()
    {
        var p = Profile(totalMessages: 120, batchSize: 100);
        RunTargets.NextBatchSize(p, 0, 0, 100).ShouldBe(100);
        RunTargets.NextBatchSize(p, 100, 0, 100).ShouldBe(20);
        RunTargets.NextBatchSize(p, 120, 0, 100).ShouldBe(0);
    }

    [Fact]
    public void NextBatchSize_ClampedByRemainingBytes()
    {
        // 1 сообщение ≈ 100 байт body; осталось 250 байт → 3 сообщения, но batch=2
        var p = Profile(totalBytes: 1250, batchSize: 2);
        RunTargets.NextBatchSize(p, 0, 1000, 100).ShouldBe(2);
        // осталось ровно 0 — ничего не диспетчеризуем
        RunTargets.NextBatchSize(p, 0, 1250, 100).ShouldBe(0);
    }

    [Fact]
    public void ReachedTarget_ReportsWhichTarget()
    {
        var start = Start;
        RunTargets.ReachedTarget(Profile(totalMessages: 5), 5, 0, start.AddSeconds(1), start).ShouldBe("TargetMessages");
        RunTargets.ReachedTarget(Profile(totalBytes: 100), 0, 100, start.AddSeconds(1), start).ShouldBe("TargetPayloadBytes");
        RunTargets.ReachedTarget(Profile(durationSec: 10), 0, 0, start.AddSeconds(11), start).ShouldBe("TargetDuration");
        RunTargets.ReachedTarget(Profile(runUntilStopped: true), 100, 100, start.AddHours(1), start).ShouldBeNull();
    }
}
