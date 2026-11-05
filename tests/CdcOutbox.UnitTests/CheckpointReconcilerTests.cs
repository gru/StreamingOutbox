using CdcOutbox.Core.Processing;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class CheckpointReconcilerTests
{
    [Fact]
    public void NoCheckpoint_MissingSlot_Allowed()
    {
        CheckpointReconciler.FindIssue(slotExists: false, confirmedFlushDiffBytes: null, checkpoint: null).ShouldBeNull();
    }

    [Fact]
    public void NoCheckpoint_ExistingSlot_Allowed()
    {
        CheckpointReconciler.FindIssue(slotExists: true, confirmedFlushDiffBytes: null, checkpoint: null).ShouldBeNull();
    }

    [Fact]
    public void CheckpointBehindFlush_Allowed()
    {
        CheckpointReconciler.FindIssue(slotExists: true, confirmedFlushDiffBytes: 4096, checkpoint: 0x100).ShouldBeNull();
    }

    [Fact]
    public void CheckpointEqualsFlush_Allowed()
    {
        CheckpointReconciler.FindIssue(slotExists: true, confirmedFlushDiffBytes: 0, checkpoint: 0x100).ShouldBeNull();
    }

    [Fact]
    public void MissingSlotWithCheckpoint_Forbidden()
    {
        CheckpointReconciler.FindIssue(slotExists: false, confirmedFlushDiffBytes: null, checkpoint: 0x100)
            .ShouldNotBeNull()
            .ShouldContain("автопересоздание запрещено");
    }

    [Fact]
    public void CheckpointAheadOfFlush_Allowed()
    {
        CheckpointReconciler.FindIssue(slotExists: true, confirmedFlushDiffBytes: -100, checkpoint: 0x200)
            .ShouldBeNull();
    }

    [Fact]
    public void CheckpointWithEmptyFlush_Forbidden()
    {
        CheckpointReconciler.FindIssue(slotExists: true, confirmedFlushDiffBytes: null, checkpoint: 0x100)
            .ShouldNotBeNull()
            .ShouldContain("не с чем сравнить");
    }
}
