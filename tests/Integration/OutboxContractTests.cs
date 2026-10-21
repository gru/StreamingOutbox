using System.Text.Json;
using Xunit;
using KafkaValidator.Validation;
using Writer.Generation;

namespace IntegrationTests;

/// <summary>
/// Контракт формата «Writer → Kafka → KafkaValidator» без стенда: сообщение,
/// созданное MessageFactory (body bytea + key bytea), разбирается парсером
/// валидатора (OutboxMessageBody) — до самого вердикта. Value в Kafka — это
/// body outbox-строки as-is (bytea pass-through), без промежуточного конверта.
/// </summary>
public class OutboxContractTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 10, 21, 12, 34, 56, 789, TimeSpan.Zero);

    private static long[] Ids(int count, long start = 1000) =>
        Enumerable.Range(0, count).Select(i => start + i).ToArray();

    [Fact]
    public void MessageFactoryBody_ParsedByValidator_IdRunIdSurvive()
    {
        var runId = Guid.NewGuid();
        var (messages, _) = MessageFactory.CreateBatch(Ids(25), runId, payloadBytes: 64);

        foreach (var m in messages)
        {
            var ok = OutboxMessageBody.TryParse(m.Body, out var parsed);

            Assert.True(ok, $"body id={m.Id} не разобрался");
            Assert.Equal(m.Id, parsed!.Id);
            Assert.Equal(runId, parsed.RunId);
            // Writer пишет timestamp с усечением до миллисекунд (yyyy-MM-ddTHH:mm:ss.fffZ)
            var expectedTs = new DateTimeOffset(
                m.CreatedAt.Ticks - m.CreatedAt.Ticks % TimeSpan.TicksPerMillisecond, m.CreatedAt.Offset);
            Assert.Equal(expectedTs, parsed.TimestampUtc);
        }
    }

    [Fact]
    public void FullCycle_MessageFactoryToVerdict_PassWithoutLoss()
    {
        var runId = Guid.NewGuid();
        var (messages, _) = MessageFactory.CreateBatch(Ids(50), runId, payloadBytes: 128);
        var registry = new RunRegistry();

        foreach (var m in messages)
        {
            var ok = OutboxMessageBody.TryParse(m.Body, out var parsed);
            Assert.True(ok);
            registry.Record(parsed!, Now);
        }

        var expected = new ExpectedSet(
            runId, messages.Select(m => m.Id).ToHashSet(), new HashSet<long>(), new HashSet<long>(),
            ExpectedSet.SourceWriter);
        var verdict = VerdictComputer.Compute(runId, registry.GetRunDetail(runId), expected, Now);

        Assert.Equal("pass", verdict.Verdict);
        Assert.Equal(50, verdict.Kafka.Records);
        Assert.Equal(50, verdict.Kafka.UniqueIds);
        Assert.Equal(0, verdict.Kafka.DuplicateRecords);
        Assert.Equal(0, verdict.MissingWrittenIdsCount);
    }

    [Fact]
    public void FullCycle_OneMessageLostInKafka_FailWithProvenLoss()
    {
        var runId = Guid.NewGuid();
        var (messages, _) = MessageFactory.CreateBatch(Ids(10), runId, payloadBytes: 32);
        var registry = new RunRegistry();

        foreach (var m in messages.Skip(1)) // первая запись «потерялась» до Kafka
        {
            var ok = OutboxMessageBody.TryParse(m.Body, out var parsed);
            Assert.True(ok);
            registry.Record(parsed!, Now);
        }

        var expected = new ExpectedSet(
            runId, messages.Select(m => m.Id).ToHashSet(), new HashSet<long>(), new HashSet<long>(),
            ExpectedSet.SourceExplicit);
        var verdict = VerdictComputer.Compute(runId, registry.GetRunDetail(runId), expected, Now);

        Assert.Equal("fail", verdict.Verdict);
        Assert.Equal(1, verdict.MissingWrittenIdsCount);
        Assert.Equal(messages[0].Id, Assert.Single(verdict.MissingWrittenIdsSample));
    }

    [Fact]
    public void FullCycle_DuplicateDelivery_StillPass()
    {
        // at-least-once: одна запись доставлена дважды — дубликат, не потеря
        var runId = Guid.NewGuid();
        var (messages, _) = MessageFactory.CreateBatch(Ids(5), runId, payloadBytes: 16);
        var registry = new RunRegistry();

        foreach (var m in messages)
        {
            var ok = OutboxMessageBody.TryParse(m.Body, out var parsed);
            Assert.True(ok);
            registry.Record(parsed!, Now);
        }
        var okDup = OutboxMessageBody.TryParse(messages[0].Body, out var dup);
        Assert.True(okDup);
        registry.Record(dup!, Now);

        var expected = new ExpectedSet(
            runId, messages.Select(m => m.Id).ToHashSet(), new HashSet<long>(), new HashSet<long>(),
            ExpectedSet.SourceWriter);
        var verdict = VerdictComputer.Compute(runId, registry.GetRunDetail(runId), expected, Now);

        Assert.Equal("pass", verdict.Verdict);
        Assert.Equal(1, verdict.Kafka.DuplicateRecords);
    }

    [Fact]
    public void WriterRunIdsDto_JsonContract_ValidatorDeserializes()
    {
        // Writer отвечает camelCase (дефолт ASP.NET Core: JsonSerializerDefaults.Web);
        // валидатор читает case-insensitive — тест фиксирует контракт обеих сторон
        var dto = new RunIdsDto(
            RunId: Guid.NewGuid(),
            WrittenCount: 3,
            AmbiguousCount: 1,
            FailedCount: 0,
            WrittenIds: [1001, 1002, 1003],
            AmbiguousIds: [1004],
            FailedIds: []);

        var json = JsonSerializer.Serialize(dto, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var parsed = JsonSerializer.Deserialize<WriterRunIds>(json, options);

        Assert.NotNull(parsed);
        Assert.Equal(dto.RunId, parsed!.RunId);
        Assert.Equal(3, parsed.WrittenCount);
        Assert.Equal(dto.WrittenIds, parsed.WrittenIds);
        Assert.Equal(dto.AmbiguousIds, parsed.AmbiguousIds);
        Assert.Empty(parsed.FailedIds);
    }
}
