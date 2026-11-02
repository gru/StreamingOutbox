using CdcOutbox.Core.Replication;
using Npgsql;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class OutboxSlotReaderTests
{
    [Fact]
    public void NormalizeIdentifier_TrimsAndLowercasesLikeUnquotedSqlIdentifier()
    {
        OutboxSlotReader.NormalizeIdentifier("Outbox ").ShouldBe("outbox");
        OutboxSlotReader.NormalizeIdentifier(" Messages").ShouldBe("messages");
        OutboxSlotReader.NormalizeIdentifier("outbox").ShouldBe("outbox");
    }

    [Fact]
    public void IsOutboxRelation_MatchingNamespaceAndTable_True()
    {
        OutboxSlotReader.IsOutboxRelation("outbox", "messages", "outbox", "messages").ShouldBeTrue();
    }

    [Fact]
    public void IsOutboxRelation_OtherTable_False()
    {
        OutboxSlotReader.IsOutboxRelation("outbox", "notes", "outbox", "messages").ShouldBeFalse();
    }

    [Fact]
    public void IsOutboxRelation_OtherSchema_False()
    {
        OutboxSlotReader.IsOutboxRelation("public", "messages", "outbox", "messages").ShouldBeFalse();
    }

    [Fact]
    public void IsOutboxRelation_WireNameWithDifferentCase_False()
    {
        OutboxSlotReader.IsOutboxRelation("outbox", "Messages", "outbox", "messages").ShouldBeFalse();
    }

    [Fact]
    public void FindForeignRelationWarning_NamesRelationAndPublication()
    {
        var warning = OutboxSlotReader.FindForeignRelationWarning("it_s_abc", "notes", "it_pub");

        warning.ShouldContain("it_s_abc.notes");
        warning.ShouldContain("it_pub");
        warning.ShouldContain("публика");
    }

    [Fact]
    public void FindReadingIssue_SlotBusy_TransientWithSqlState()
    {
        var issue = OutboxSlotReader.FindReadingIssue(new PostgresException(
            "replication slot is active for PID", "ERROR", "ERROR", "55006"));

        issue.IsTerminal.ShouldBeFalse();
        issue.Description.ShouldContain("55006");
    }

    [Fact]
    public void FindReadingIssue_SlotMissing_TransientAndForbidsRecreation()
    {
        var issue = OutboxSlotReader.FindReadingIssue(new PostgresException(
            "replication slot does not exist", "ERROR", "ERROR", PostgresErrorCodes.UndefinedObject));

        issue.IsTerminal.ShouldBeFalse();
        issue.Description.ShouldContain("42704");
        issue.Description.ShouldContain("пересоздание");
        issue.Description.ShouldContain("Продолжаю попытки");
    }

    [Fact]
    public void FindReadingIssue_SlotInvalidated_Terminal()
    {
        var issue = OutboxSlotReader.FindReadingIssue(new PostgresException(
            "logical replication slot \"outbox_slot\" has been invalidated due to WAL removal",
            "ERROR", "ERROR", PostgresErrorCodes.ObjectNotInPrerequisiteState));

        issue.IsTerminal.ShouldBeTrue();
        issue.Description.ShouldContain("инвалидирован");
    }

    [Fact]
    public void FindReadingIssue_SlotInvalidationMessageCaseInsensitive_Terminal()
    {
        var issue = OutboxSlotReader.FindReadingIssue(new PostgresException(
            "slot was Invalidated", "ERROR", "ERROR", "XX000"));

        issue.IsTerminal.ShouldBeTrue();
    }

    [Fact]
    public void FindReadingIssue_OtherPostgresError_TransientWithSqlState()
    {
        var issue = OutboxSlotReader.FindReadingIssue(new PostgresException(
            "boom", "ERROR", "ERROR", "42P01"));

        issue.IsTerminal.ShouldBeFalse();
        issue.Description.ShouldContain("42P01");
    }

    [Fact]
    public void FindReadingIssue_ReadOnly_NamesNodeState()
    {
        var issue = OutboxSlotReader.FindReadingIssue(new PostgresException(
            "cannot execute START_REPLICATION in a read-only transaction", "ERROR", "ERROR",
            PostgresErrorCodes.ReadOnlySqlTransaction));

        issue.IsTerminal.ShouldBeFalse();
        issue.Description.ShouldContain("25006");
    }

    [Fact]
    public void FindReadingIssue_NpgsqlException_Network()
    {
        OutboxSlotReader.FindReadingIssue(new NpgsqlException("socket closed"))
            .Description.ShouldContain("сетевая");
    }

    [Fact]
    public void FindReadingIssue_IoException_Network()
    {
        OutboxSlotReader.FindReadingIssue(new IOException("connection reset"))
            .Description.ShouldContain("сетевая");
    }

    [Fact]
    public void FindReadingIssue_DecodeError_MentionsReread()
    {
        var formatIssue = OutboxSlotReader.FindReadingIssue(new FormatException("Колонка topic: NULL"));
        formatIssue.IsTerminal.ShouldBeFalse();
        formatIssue.Description.ShouldContain("декодирования");
        formatIssue.Description.ShouldContain("перечитано");

        var planIssue = OutboxSlotReader.FindReadingIssue(
            new InvalidOperationException("Таблица outbox не соответствует контракту"));
        planIssue.Description.ShouldContain("декодирования");
    }

    [Fact]
    public void FindReadingIssue_Unexpected_FallsBackToGenericText()
    {
        var issue = OutboxSlotReader.FindReadingIssue(new ArithmeticException("boom"));

        issue.IsTerminal.ShouldBeFalse();
        issue.Description.ShouldContain("Неожиданная");
    }
}
