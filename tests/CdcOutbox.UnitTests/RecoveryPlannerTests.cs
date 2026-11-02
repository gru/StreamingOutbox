using CdcOutbox.Core.Processing;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class RecoveryPlannerTests
{
    [Fact]
    public void Plan_NoCheckpoint_NoSlot_NotNeeded()
    {
        var decision = RecoveryPlanner.Plan(checkpoint: null, slotExists: false, confirmedFlush: null);

        decision.Action.ShouldBe(RecoveryAction.NotNeeded);
        decision.Description.ShouldNotBeNullOrWhiteSpace();
    }

    [Fact]
    public void Plan_NoCheckpoint_ExistingSlot_NotNeeded()
    {
        var decision = RecoveryPlanner.Plan(checkpoint: null, slotExists: true, confirmedFlush: 0x100);

        decision.Action.ShouldBe(RecoveryAction.NotNeeded);
    }

    [Fact]
    public void Plan_MissingSlotWithCheckpoint_CreatesSlot()
    {
        var decision = RecoveryPlanner.Plan(checkpoint: 0x200, slotExists: false, confirmedFlush: null);

        decision.Action.ShouldBe(RecoveryAction.CreateSlot);
        decision.Description.ShouldContain("текущей позиции");
    }

    [Fact]
    public void Plan_ExistingSlotWithEmptyFlush_Rejects()
    {
        var decision = RecoveryPlanner.Plan(checkpoint: 0x200, slotExists: true, confirmedFlush: null);

        decision.Action.ShouldBe(RecoveryAction.Reject);
        decision.Description.ShouldContain("confirmed_flush пуст");
        decision.Description.ShouldContain("вручную");
    }

    [Fact]
    public void Plan_CheckpointBehindFlush_NotNeeded()
    {
        var decision = RecoveryPlanner.Plan(checkpoint: 0x100, slotExists: true, confirmedFlush: 0x500);

        decision.Action.ShouldBe(RecoveryAction.NotNeeded);
        decision.Description.ShouldContain("рассогласования нет");
    }

    [Fact]
    public void Plan_CheckpointEqualsFlush_NotNeeded()
    {
        var decision = RecoveryPlanner.Plan(checkpoint: 0x500, slotExists: true, confirmedFlush: 0x500);

        decision.Action.ShouldBe(RecoveryAction.NotNeeded);
    }

    [Fact]
    public void Plan_CheckpointAheadOfFlush_AdoptsSlotPosition()
    {
        var decision = RecoveryPlanner.Plan(checkpoint: 0x900, slotExists: true, confirmedFlush: 0x500);

        decision.Action.ShouldBe(RecoveryAction.AdoptSlotPosition);
        decision.Description.ShouldContain("опережает");
        decision.Description.ShouldContain("пропускается");
    }
}
