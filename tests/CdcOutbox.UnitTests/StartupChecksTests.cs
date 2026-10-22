using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Replication;
using Confluent.Kafka;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class StartupChecksTests
{
    [Fact]
    public void FindExistingSlotIssue_PluginMismatch_NamesSlotAndExpectedPlugin()
    {
        var issue = OutboxSlotProvisioner.FindExistingSlotIssue("outbox_slot", "test_decoding");

        issue.ShouldNotBeNull();
        issue.ShouldContain("outbox_slot");
        issue.ShouldContain("pgoutput");
        issue.ShouldContain("test_decoding");
    }

    [Fact]
    public void FindExistingSlotIssue_PluginEmpty_NamesSlot()
    {
        var issue = OutboxSlotProvisioner.FindExistingSlotIssue("outbox_slot", null);

        issue.ShouldNotBeNull();
        issue.ShouldContain("outbox_slot");
    }

    [Fact]
    public void FindExistingSlotIssue_PgoutputSlot_ReturnsNull()
    {
        OutboxSlotProvisioner.FindExistingSlotIssue("outbox_slot", "pgoutput").ShouldBeNull();
    }

    [Fact]
    public void FindSlotFailoverWarning_TurnedOff_NamesSlotAndConsequence()
    {
        var warning = OutboxSlotProvisioner.FindSlotFailoverWarning("outbox_slot", false);

        warning.ShouldNotBeNull();
        warning.ShouldContain("outbox_slot");
        warning.ShouldContain("promotion");
    }

    [Fact]
    public void FindSlotFailoverWarning_FlagUnknown_ReturnsWarning()
    {
        OutboxSlotProvisioner.FindSlotFailoverWarning("outbox_slot", null).ShouldNotBeNull();
    }

    [Fact]
    public void FindSlotFailoverWarning_TurnedOn_ReturnsNull()
    {
        OutboxSlotProvisioner.FindSlotFailoverWarning("outbox_slot", true).ShouldBeNull();
    }

    [Fact]
    public void MapSlotCreationIssue_WalLevelError_HintsClusterDecodingConfiguration()
    {
        var issue = OutboxSlotProvisioner.MapSlotCreationIssue(
            "55000", "logical decoding requires \"wal_level\" >= \"logical\"");

        issue.ShouldNotBeNull();
        issue.ShouldContain("wal_level");
    }

    [Fact]
    public void MapSlotCreationIssue_WalLevelErrorIsCaseInsensitive()
    {
        var issue = OutboxSlotProvisioner.MapSlotCreationIssue(
            "55000", "LOGICAL DECODING REQUIRES WAL_LEVEL >= LOGICAL");

        issue.ShouldNotBeNull();
        issue.ShouldContain("wal_level");
    }

    [Fact]
    public void MapSlotCreationIssue_DuplicateSlot_ReturnsNull()
    {
        OutboxSlotProvisioner.MapSlotCreationIssue(
            "42710", "replication slot \"outbox_slot\" already exists").ShouldBeNull();
    }

    [Fact]
    public void MapSlotCreationIssue_UnrecognizedStateOrMessage_ReturnsNull()
    {
        OutboxSlotProvisioner.MapSlotCreationIssue(
            "55000", "something unrelated").ShouldBeNull();
        OutboxSlotProvisioner.MapSlotCreationIssue(
            "42P13", "invalid parameter").ShouldBeNull();
        OutboxSlotProvisioner.MapSlotCreationIssue(
            null, "logical decoding requires wal_level >= logical").ShouldBeNull();
    }

    [Fact]
    public void FindPublicationIssue_MissingPublication_NamesPublicationAndExpectedConfiguration()
    {
        var state = new PublicationState(false, false, false, false, false, false);

        var issue = PublicationVerifier.FindPublicationIssue(state, "outbox_publication", "outbox", "messages");

        issue.ShouldNotBeNull();
        issue.ShouldContain("outbox_publication");
        issue.ShouldContain("publish='insert'");
        issue.ShouldContain("outbox.messages");
    }

    [Fact]
    public void FindPublicationIssue_InsertsDisabled_NamesInsertRequirement()
    {
        var state = new PublicationState(true, false, false, false, false, true);

        var issue = PublicationVerifier.FindPublicationIssue(state, "outbox_publication", "outbox", "messages");

        issue.ShouldNotBeNull();
        issue.ShouldContain("INSERT");
        issue.ShouldContain("outbox_publication");
    }

    [Fact]
    public void FindExtraOperationsWarning_ExtraOpsPublished_ReturnsWarningNamingPublication()
    {
        var state = new PublicationState(true, true, true, false, false, true);

        var warning = PublicationVerifier.FindExtraOperationsWarning(state, "outbox_publication");

        warning.ShouldNotBeNull();
        warning.ShouldContain("outbox_publication");
        warning.ShouldContain("pubupdate=t");
        warning.ShouldContain("pubdelete=f");
    }

    [Fact]
    public void FindExtraOperationsWarning_TruncateOnly_ReturnsWarning()
    {
        var state = new PublicationState(true, true, false, false, true, true);

        PublicationVerifier.FindExtraOperationsWarning(state, "outbox_publication")
            .ShouldNotBeNull().ShouldContain("pubtruncate=t");
    }

    [Fact]
    public void FindExtraOperationsWarning_InsertOnly_ReturnsNull()
    {
        var state = new PublicationState(true, true, false, false, false, true);

        PublicationVerifier.FindExtraOperationsWarning(state, "outbox_publication").ShouldBeNull();
    }

    [Fact]
    public void FindPublicationIssue_TableMissing_NamesTableAndPublication()
    {
        var state = new PublicationState(true, true, false, false, false, false);

        var issue = PublicationVerifier.FindPublicationIssue(state, "outbox_publication", "outbox", "messages");

        issue.ShouldNotBeNull();
        issue.ShouldContain("outbox.messages");
        issue.ShouldContain("outbox_publication");
    }

    [Fact]
    public void FindPublicationIssue_ContractPublication_ReturnsNull()
    {
        var state = new PublicationState(true, true, false, false, false, true);

        PublicationVerifier.FindPublicationIssue(state, "outbox_publication", "outbox", "messages").ShouldBeNull();
    }

    [Fact]
    public void FindTopicIssue_TopicPresent_ReturnsNull()
    {
        var metadata = ClusterMetadata("outbox.events", "cdc-checkpoints");

        CheckpointTopicVerifier.FindTopicIssue(metadata, "cdc-checkpoints").ShouldBeNull();
    }

    [Fact]
    public void FindTopicIssue_TopicAbsent_NamesTopicAndRemedy()
    {
        var metadata = ClusterMetadata("outbox.events");

        var issue = CheckpointTopicVerifier.FindTopicIssue(metadata, "cdc-checkpoints");

        issue.ShouldNotBeNull();
        issue.ShouldContain("cdc-checkpoints");
        issue.ShouldContain("создай");
    }

    private static Metadata ClusterMetadata(params string[] topics)
    {
        var topicMetadata = topics
            .Select(topic => new TopicMetadata(topic, new List<PartitionMetadata>(), new Error(ErrorCode.NoError)))
            .ToList();
        return new Metadata(
            new List<BrokerMetadata> { new(1, "kafka1", 9092) },
            topicMetadata,
            1,
            "kafka1");
    }
}
