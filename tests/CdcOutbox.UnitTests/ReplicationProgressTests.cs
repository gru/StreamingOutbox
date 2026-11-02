using CdcOutbox.Core.Processing;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class ReplicationProgressTests
{
    [Fact]
    public void MarkPositions_MoveOnlyForward()
    {
        var progress = new ReplicationProgress();

        progress.MarkRead(5);
        progress.MarkRead(3);
        progress.ReadPosition.ShouldBe(5UL);

        progress.MarkDelivered(7);
        progress.MarkDelivered(4);
        progress.DeliveredPosition.ShouldBe(7UL);

        progress.MarkCheckpointed(9, DateTime.UtcNow);
        progress.MarkCheckpointed(2, DateTime.UtcNow);
        progress.CheckpointedPosition.ShouldBe(9UL);
    }

    [Fact]
    public void LastCheckpointAtUtc_IsInitiallyNull_AndKeepsLatestTimestamp()
    {
        var progress = new ReplicationProgress();
        progress.LastCheckpointAtUtc.ShouldBeNull();

        var early = new DateTime(2026, 11, 1, 6, 0, 0, DateTimeKind.Utc);
        var late = early.AddHours(1);
        progress.MarkCheckpointed(1, late);
        progress.MarkCheckpointed(2, early);

        progress.LastCheckpointAtUtc.ShouldBe(late);
    }

    [Fact]
    public async Task WaitForDelivery_TimesOutWithoutMarks()
    {
        var progress = new ReplicationProgress();

        var signaled = await progress.WaitForDeliveryAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);

        signaled.ShouldBeFalse();
    }

    [Fact]
    public async Task WaitForDelivery_ReturnsAfterMarkDelivered()
    {
        var progress = new ReplicationProgress();
        var waiting = progress.WaitForDeliveryAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

        waiting.IsCompleted.ShouldBeFalse();
        progress.MarkDelivered(1);

        (await waiting).ShouldBeTrue();
    }

    [Fact]
    public async Task WaitForCheckpoint_ReturnsAfterMarkCheckpointed()
    {
        var progress = new ReplicationProgress();
        var waiting = progress.WaitForCheckpointAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

        waiting.IsCompleted.ShouldBeFalse();
        progress.MarkCheckpointed(1, DateTime.UtcNow);

        (await waiting).ShouldBeTrue();
    }
}
