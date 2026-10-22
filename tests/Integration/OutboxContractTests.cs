using System.Text.Json;
using KafkaValidator.Contracts;
using KafkaValidator.Validation;
using Shouldly;
using Writer.Contracts;
using Writer.Generation;
using Xunit;

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

            ok.ShouldBeTrue($"body id={m.Id} не разобрался");
            parsed!.Id.ShouldBe(m.Id);
            parsed.RunId.ShouldBe(runId);
            // Writer пишет timestamp с усечением до миллисекунд (yyyy-MM-ddTHH:mm:ss.fffZ)
            var expectedTs = new DateTimeOffset(
                m.CreatedAt.Ticks - m.CreatedAt.Ticks % TimeSpan.TicksPerMillisecond, m.CreatedAt.Offset);
            parsed.TimestampUtc.ShouldBe(expectedTs);
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
            ok.ShouldBeTrue();
            registry.Record(parsed!, Now);
        }

        var expected = new ExpectedSet(
            runId, messages.Select(m => m.Id).ToHashSet(), new HashSet<long>(), new HashSet<long>(),
            ExpectedSet.SourceWriter);
        var verdict = VerdictComputer.Compute(runId, registry.GetRunDetail(runId), expected, Now);

        verdict.Verdict.ShouldBe("pass");
        verdict.Kafka.Records.ShouldBe(50);
        verdict.Kafka.UniqueIds.ShouldBe(50);
        verdict.Kafka.DuplicateRecords.ShouldBe(0);
        verdict.MissingWrittenIdsCount.ShouldBe(0);
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
            ok.ShouldBeTrue();
            registry.Record(parsed!, Now);
        }

        var expected = new ExpectedSet(
            runId, messages.Select(m => m.Id).ToHashSet(), new HashSet<long>(), new HashSet<long>(),
            ExpectedSet.SourceExplicit);
        var verdict = VerdictComputer.Compute(runId, registry.GetRunDetail(runId), expected, Now);

        verdict.Verdict.ShouldBe("fail");
        verdict.MissingWrittenIdsCount.ShouldBe(1);
        Assert.Single(verdict.MissingWrittenIdsSample).ShouldBe(messages[0].Id);
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
            ok.ShouldBeTrue();
            registry.Record(parsed!, Now);
        }
        var okDup = OutboxMessageBody.TryParse(messages[0].Body, out var dup);
        okDup.ShouldBeTrue();
        registry.Record(dup!, Now);

        var expected = new ExpectedSet(
            runId, messages.Select(m => m.Id).ToHashSet(), new HashSet<long>(), new HashSet<long>(),
            ExpectedSet.SourceWriter);
        var verdict = VerdictComputer.Compute(runId, registry.GetRunDetail(runId), expected, Now);

        verdict.Verdict.ShouldBe("pass");
        verdict.Kafka.DuplicateRecords.ShouldBe(1);
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

        parsed.ShouldNotBeNull();
        parsed!.RunId.ShouldBe(dto.RunId);
        parsed.WrittenCount.ShouldBe(3);
        parsed.WrittenIds.ShouldBe(dto.WrittenIds);
        parsed.AmbiguousIds.ShouldBe(dto.AmbiguousIds);
        parsed.FailedIds.ShouldBeEmpty();
    }
}
