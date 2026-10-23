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
    public void ClassifyReadingIssue_SlotBusy_NamesState()
    {
        var issue = OutboxSlotReader.ClassifyReadingIssue(new PostgresException(
            "replication slot is active for PID", "ERROR", "ERROR", "55006"));

        issue.ShouldContain("55006");
    }

    [Fact]
    public void ClassifyReadingIssue_SlotMissing_ForbidsRecreation()
    {
        var issue = OutboxSlotReader.ClassifyReadingIssue(new PostgresException(
            "replication slot does not exist", "ERROR", "ERROR", PostgresErrorCodes.UndefinedObject));

        issue.ShouldContain("42704");
        issue.ShouldContain("пересоздание");
    }

    [Fact]
    public void ClassifyReadingIssue_ReadOnly_NamesNodeState()
    {
        var issue = OutboxSlotReader.ClassifyReadingIssue(new PostgresException(
            "cannot execute START_REPLICATION in a read-only transaction", "ERROR", "ERROR",
            PostgresErrorCodes.ReadOnlySqlTransaction));

        issue.ShouldContain("25006");
    }

    [Fact]
    public void ClassifyReadingIssue_OtherPostgresError_NamesSqlState()
    {
        var issue = OutboxSlotReader.ClassifyReadingIssue(new PostgresException(
            "boom", "ERROR", "ERROR", "42P01"));

        issue.ShouldContain("42P01");
    }

    [Fact]
    public void ClassifyReadingIssue_NpgsqlException_Network()
    {
        var issue = OutboxSlotReader.ClassifyReadingIssue(new NpgsqlException("socket closed"));

        issue.ShouldContain("сетевая");
    }

    [Fact]
    public void ClassifyReadingIssue_IoException_Network()
    {
        var issue = OutboxSlotReader.ClassifyReadingIssue(new IOException("connection reset"));

        issue.ShouldContain("сетевая");
    }

    [Fact]
    public void ClassifyReadingIssue_DecodeError_MentionsReread()
    {
        var formatIssue = OutboxSlotReader.ClassifyReadingIssue(new FormatException("Колонка topic: NULL"));
        formatIssue.ShouldContain("декодирования");
        formatIssue.ShouldContain("перечитано");

        var planIssue = OutboxSlotReader.ClassifyReadingIssue(
            new InvalidOperationException("Таблица outbox не соответствует контракту"));
        planIssue.ShouldContain("декодирования");
    }

    [Fact]
    public void ClassifyReadingIssue_Unexpected_FallsBackToGenericText()
    {
        var issue = OutboxSlotReader.ClassifyReadingIssue(new ArithmeticException("boom"));

        issue.ShouldContain("Неожиданная");
    }
}
