using CdcOutbox.Core.Processing;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class IdleActivityTrackerTests
{
    private static readonly TimeSpan Threshold = TimeSpan.FromMilliseconds(50);

    [Fact]
    public void TryBeginWarning_BeforeThreshold_ReturnsNull()
    {
        var tracker = new IdleActivityTracker();

        tracker.TryBeginWarning(Threshold).ShouldBeNull();
    }

    [Fact]
    public async Task TryBeginWarning_AfterThreshold_ReturnsIdle()
    {
        var tracker = new IdleActivityTracker();

        await Task.Delay(60);

        var idle = tracker.TryBeginWarning(Threshold);
        idle.ShouldNotBeNull();
        idle.Value.ShouldBeGreaterThanOrEqualTo(Threshold);
    }

    [Fact]
    public async Task TryBeginWarning_TwiceInRow_SecondIsNull()
    {
        var tracker = new IdleActivityTracker();

        await Task.Delay(60);
        tracker.TryBeginWarning(Threshold).ShouldNotBeNull();
        tracker.TryBeginWarning(Threshold).ShouldBeNull();
    }

    [Fact]
    public async Task TryBeginWarning_NextInterval_WarnsAgain()
    {
        var tracker = new IdleActivityTracker();

        await Task.Delay(60);
        tracker.TryBeginWarning(Threshold).ShouldNotBeNull();

        await Task.Delay(60);
        tracker.TryBeginWarning(Threshold).ShouldNotBeNull();
    }

    [Fact]
    public async Task MarkActivity_ResetsTimer()
    {
        var tracker = new IdleActivityTracker();

        await Task.Delay(60);
        tracker.TryBeginWarning(Threshold).ShouldNotBeNull();

        tracker.MarkActivity();
        tracker.TryBeginWarning(Threshold).ShouldBeNull();

        await Task.Delay(60);
        tracker.TryBeginWarning(Threshold).ShouldNotBeNull();
    }
}
