using KafkaValidator.Validation;
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

        Assert.Equal(2, registry.RunsSeen);
        Assert.Equal(2, registry.GetRun(Run1)!.Records);
        Assert.Equal(1, registry.GetRun(Run2)!.Records);
        Assert.Null(registry.GetRun(Guid.NewGuid()));
    }

    [Fact]
    public void Record_SameIdTwice_DuplicatesCounted()
    {
        var registry = new RunRegistry();
        registry.Record(Msg(7, Run1), T0);
        registry.Record(Msg(7, Run1), T0);

        var run = registry.GetRun(Run1)!;
        Assert.Equal(2, run.Records);
        Assert.Equal(1, run.UniqueIds);
        Assert.Equal(1, run.DuplicateRecords);
    }

    [Fact]
    public void Record_NoRunId_SkippedFromRuns_ButCountedInTotal()
    {
        // ручная запись в outbox без body.runId: общий счётчик потребления растёт,
        // в прогоны (и в вердикты) не попадает
        var registry = new RunRegistry();
        registry.Record(Msg(1), T0);
        registry.Record(Msg(2, Run1), T0);

        Assert.Equal(1, registry.RunsSeen);
        Assert.Equal(2, registry.RecordsTotal);
        Assert.Equal(1, registry.SkippedNoRunId);
        Assert.NotNull(registry.LastRecordUtc);
    }

    [Fact]
    public void RecordParseError_CountedSeparately()
    {
        var registry = new RunRegistry();
        registry.RecordParseError();

        Assert.Equal(1, registry.RecordsTotal);
        Assert.Equal(1, registry.ParseErrors);
        Assert.Equal(0, registry.RunsSeen);
        Assert.Null(registry.LastRecordUtc);
    }

    [Fact]
    public void ListRuns_ChronologicalByFirstSeen()
    {
        var registry = new RunRegistry();
        registry.Record(Msg(1, Run2), T0.AddSeconds(5));
        registry.Record(Msg(2, Run1), T0);

        var runs = registry.ListRuns();
        Assert.Equal([Run1, Run2], runs.Select(r => r.RunId));
        Assert.Equal(T0, runs[0].FirstSeenUtc);
        Assert.Equal(T0.AddSeconds(5), runs[1].FirstSeenUtc);
    }

    [Fact]
    public void GetRunDetail_ExposesIdSetForVerdict()
    {
        var registry = new RunRegistry();
        registry.Record(Msg(42, Run1), T0);

        var detail = registry.GetRunDetail(Run1)!;
        Assert.Equal((IReadOnlySet<long>)new HashSet<long> { 42 }, detail.UniqueIds);
    }
}
