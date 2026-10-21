using Xunit;
using KafkaValidator.Validation;

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

public class OutboxMessageBodyTests
{
    private static readonly Guid RunId = Guid.Parse("00000000-0000-7000-8000-0000000000a1");

    private static byte[] Utf8(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Fact]
    public void TryParse_ValidWriterBody_ParsesIdRunIdTimestamp()
    {
        var ok = OutboxMessageBody.TryParse(
            Utf8($$"""{"id":123456789012,"runId":"{{RunId}}","eventType":"TestEvent","payload":"xyz","timestamp":"2026-10-21T12:34:56.789Z"}"""),
            out var parsed);

        Assert.True(ok);
        Assert.Equal(123456789012L, parsed!.Id);
        Assert.Equal(RunId, parsed.RunId);
        Assert.Equal(DateTimeOffset.Parse("2026-10-21T12:34:56.789Z"), parsed.TimestampUtc);
    }

    [Fact]
    public void TryParse_WithoutRunId_OkWithNullRunId()
    {
        var ok = OutboxMessageBody.TryParse(Utf8("""{"id":5,"timestamp":"2026-10-21T12:34:56.789Z"}"""), out var parsed);

        Assert.True(ok);
        Assert.Null(parsed!.RunId); // запись уйдёт в skipped, не в parse_errors
    }

    [Theory]
    [InlineData("""{"runId":"00000000-0000-7000-8000-0000000000a1","timestamp":"2026-10-21T12:34:56.789Z"}""")] // нет id
    [InlineData("""{"id":"123","timestamp":"2026-10-21T12:34:56.789Z"}""")]    // id не число
    [InlineData("""{"id":123}""")]                                              // нет timestamp
    [InlineData("""{"id":123,"timestamp":"not-a-date"}""")]                     // битый timestamp
    [InlineData("""{"id":123,"runId":"not-a-guid","timestamp":"2026-10-21T12:34:56.789Z"}""")] // битый runId
    [InlineData("[1,2,3]")]                                                     // не объект
    [InlineData("not json at all")]                                             // битый JSON
    public void TryParse_Invalid_ReturnsFalse(string body)
    {
        Assert.False(OutboxMessageBody.TryParse(Utf8(body), out var parsed));
        Assert.Null(parsed);
    }
}
