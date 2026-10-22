using KafkaValidator.Validation;
using Shouldly;
using Xunit;

namespace KafkaValidator.UnitTests;

public class VerdictComputerTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-10-16T12:00:00Z");

    private static RunDetail Detail(params long[] ids) => new(Guid.Empty, ids.Length, ids.ToHashSet(), Now, Now);

    private static ExpectedSet Expected(
        long[] written, long[] ambiguous = null!, long[] failed = null!) => new(
        Guid.Empty,
        written.ToHashSet(),
        (ambiguous ?? []).ToHashSet(),
        (failed ?? []).ToHashSet(),
        ExpectedSet.SourceWriter);

    [Fact]
    public void Compute_FullDelivery_Pass()
    {
        var v = VerdictComputer.Compute(Guid.Empty, Detail(11, 12, 13),
            Expected([11, 12, 13]), Now);

        v.Verdict.ShouldBe("pass");
        v.Kafka.Records.ShouldBe(3);
        v.Kafka.UniqueIds.ShouldBe(3);
        v.Kafka.DuplicateRecords.ShouldBe(0);
        v.MissingWrittenIdsCount.ShouldBe(0);
        v.UnexpectedExtraIdsCount.ShouldBe(0);
        v.Expected.WrittenCount.ShouldBe(3);
        v.Expected.Source.ShouldBe(ExpectedSet.SourceWriter);
    }

    [Fact]
    public void Compute_DuplicatesAllowedByAtLeastOnce_StillPass()
    {
        // 5 записей, 3 уникальных: ретраи доставки после сбоя между delivery и ack
        var v = VerdictComputer.Compute(Guid.Empty,
            new RunDetail(Guid.Empty, 5, new HashSet<long> { 11, 12, 13 }, Now, Now),
            Expected([11, 12, 13]), Now);

        v.Verdict.ShouldBe("pass");
        v.Kafka.DuplicateRecords.ShouldBe(2);
    }

    [Fact]
    public void Compute_WrittenMissingInKafka_Fail_ProvenLoss()
    {
        var v = VerdictComputer.Compute(Guid.Empty, Detail(11, 12),
            Expected([11, 12, 13]), Now);

        v.Verdict.ShouldBe("fail");
        v.MissingWrittenIdsCount.ShouldBe(1);
        Assert.Single(v.MissingWrittenIdsSample).ShouldBe(13L);
    }

    [Fact]
    public void Compute_AmbiguousNotDelivered_IsNotProvenLoss_Pass()
    {
        // Writer не знает, был ли commit (обрыв соединения во время commit);
        // записи в Kafka нет — доказанной потери доставки нет
        var v = VerdictComputer.Compute(Guid.Empty, Detail(11),
            Expected([11], ambiguous: [91]), Now);

        v.Verdict.ShouldBe("pass");
        v.MissingWrittenIdsCount.ShouldBe(0);
        v.AmbiguousNotDeliveredCount.ShouldBe(1);
        v.AmbiguousDeliveredCount.ShouldBe(0);
    }

    [Fact]
    public void Compute_AmbiguousDelivered_Pass_Counted()
    {
        // commit всё-таки случился и доставка прошла — это нормально
        var v = VerdictComputer.Compute(Guid.Empty, Detail(11, 91),
            Expected([11], ambiguous: [91]), Now);

        v.Verdict.ShouldBe("pass");
        v.AmbiguousDeliveredCount.ShouldBe(1);
        v.UnexpectedExtraIdsCount.ShouldBe(0);
    }

    [Fact]
    public void Compute_UnexpectedExtraId_Fail()
    {
        // id вне written/ambiguous/failed — в Kafka есть то, чего Writer не отправлял
        var v = VerdictComputer.Compute(Guid.Empty, Detail(11, 99),
            Expected([11]), Now);

        v.Verdict.ShouldBe("fail");
        v.UnexpectedExtraIdsCount.ShouldBe(1);
        Assert.Single(v.UnexpectedExtraIdsSample).ShouldBe(99L);
    }

    [Fact]
    public void Compute_FailedIdDelivered_IsUnexpected_Fail()
    {
        // Writer доказал, что транзакция НЕ зафиксирована, а запись в Kafka есть
        var v = VerdictComputer.Compute(Guid.Empty, Detail(11, 77),
            Expected([11], failed: [77]), Now);

        v.Verdict.ShouldBe("fail");
        v.UnexpectedExtraIdsCount.ShouldBe(1);
    }

    [Fact]
    public void Compute_RunNeverSeenInKafka_AllWrittenMissing_Fail()
    {
        var v = VerdictComputer.Compute(Guid.Empty, tallies: null,
            Expected([11, 12]), Now);

        v.Verdict.ShouldBe("fail");
        v.Kafka.Records.ShouldBe(0);
        v.Kafka.UniqueIds.ShouldBe(0);
        v.MissingWrittenIdsCount.ShouldBe(2);
    }

    [Fact]
    public void Compute_SampleLists_CappedAt20()
    {
        var written = Enumerable.Range(0, 30).Select(i => (long)(1000 + i)).ToArray();
        var v = VerdictComputer.Compute(Guid.Empty, tallies: null, Expected(written), Now);

        v.MissingWrittenIdsCount.ShouldBe(30);
        v.MissingWrittenIdsSample.Count.ShouldBe(VerdictComputer.SampleLimit);
    }
}
