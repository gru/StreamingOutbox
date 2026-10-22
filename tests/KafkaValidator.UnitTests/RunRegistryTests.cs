using KafkaValidator.Validation;
using Shouldly;
using Xunit;

namespace KafkaValidator.UnitTests;

public class RunRegistryTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-16T12:00:00Z");
    private static readonly Guid Run1 = Guid.Parse("00000000-0000-7000-8000-0000000000a1");
    private static readonly Guid Run2 = Guid.Parse("00000000-0000-7000-8000-0000000000a2");

    private static ConsumedMessage Msg(long id, Guid? runId = null) =>
        new(id, runId, """{"eventType":"TestEvent"}""", T0);

    [Fact]
    public void Record_SeparatesRunsByRunId()
    {
        var registry = new RunRegistry();
        registry.Record(Msg(1, Run1), T0);
        registry.Record(Msg(2, Run1), T0.AddSeconds(1));
        registry.Record(Msg(3, Run2), T0.AddSeconds(2));

        registry.RunsSeen.ShouldBe(2);
        registry.GetRun(Run1)!.Records.ShouldBe(2);
        registry.GetRun(Run2)!.Records.ShouldBe(1);
        registry.GetRun(Guid.NewGuid()).ShouldBeNull();
    }

    [Fact]
    public void Record_SameIdTwice_DuplicatesCounted()
    {
        var registry = new RunRegistry();
        registry.Record(Msg(7, Run1), T0);
        registry.Record(Msg(7, Run1), T0);

        var run = registry.GetRun(Run1)!;
        run.Records.ShouldBe(2);
        run.UniqueIds.ShouldBe(1);
        run.DuplicateRecords.ShouldBe(1);
    }

    [Fact]
    public void Record_NoRunId_SkippedFromRuns_ButCountedInTotal()
    {
        // ручная запись в outbox без body.runId: общий счётчик потребления растёт,
        // в прогоны (и в вердикты) не попадает
        var registry = new RunRegistry();
        registry.Record(Msg(1), T0);
        registry.Record(Msg(2, Run1), T0);

        registry.RunsSeen.ShouldBe(1);
        registry.RecordsTotal.ShouldBe(2);
        registry.SkippedNoRunId.ShouldBe(1);
        registry.LastRecordUtc.ShouldNotBeNull();
    }

    [Fact]
    public void RecordParseError_CountedSeparately()
    {
        var registry = new RunRegistry();
        registry.RecordParseError();

        registry.RecordsTotal.ShouldBe(1);
        registry.ParseErrors.ShouldBe(1);
        registry.RunsSeen.ShouldBe(0);
        registry.LastRecordUtc.ShouldBeNull();
    }

    [Fact]
    public void ListRuns_ChronologicalByFirstSeen()
    {
        var registry = new RunRegistry();
        registry.Record(Msg(1, Run2), T0.AddSeconds(5));
        registry.Record(Msg(2, Run1), T0);

        var runs = registry.ListRuns();
        runs.Select(r => r.RunId).ShouldBe([Run1, Run2]);
        runs[0].FirstSeenUtc.ShouldBe(T0);
        runs[1].FirstSeenUtc.ShouldBe(T0.AddSeconds(5));
    }

    [Fact]
    public void GetRunDetail_ExposesIdSetForVerdict()
    {
        var registry = new RunRegistry();
        registry.Record(Msg(42, Run1), T0);

        var detail = registry.GetRunDetail(Run1)!;
        detail.UniqueIds.ShouldBe((IReadOnlySet<long>)new HashSet<long> { 42 });
    }
}
