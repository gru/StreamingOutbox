using System.Text;
using System.Text.Json;
using Shouldly;
using Writer.Generation;
using Xunit;

namespace Writer.UnitTests;

public class MessageFactoryTests
{
    private const long Id12Digits = 123456789012L; // ширина совпадает с IdDigitsEstimate

    [Fact]
    public void Create_PayloadSizeIsExactAndBytesCounted()
    {
        var runId = Guid.NewGuid();
        var msg = MessageFactory.Create(Id12Digits, runId, 512);

        using var doc = JsonDocument.Parse(msg.Body);
        var root = doc.RootElement;
        root.GetProperty("id").GetInt64().ShouldBe(Id12Digits);
        root.GetProperty("eventType").GetString().ShouldBe("TestEvent");
        root.GetProperty("runId").GetString().ShouldBe(runId.ToString());
        root.GetProperty("payload").GetString()!.Length.ShouldBe(512);
        msg.Topic.ShouldBe(MessageFactory.Topic); // 'outbox.events' — единственный разрешённый topic
        Encoding.UTF8.GetByteCount(Encoding.UTF8.GetString(msg.Body)).ShouldBe(msg.BodyBytes);
    }

    [Fact]
    public void Create_KeyIsUtf8OfDecimalId_HeadersEmptyObject()
    {
        var msg = MessageFactory.Create(Id12Digits, Guid.NewGuid(), 8);

        msg.Key.ShouldBe(Encoding.UTF8.GetBytes(Id12Digits.ToString()));
        msg.Headers.ShouldBe("{}");
    }

    [Fact]
    public void Create_TimestampIsClientUtc_Milliseconds()
    {
        var createdAt = new DateTimeOffset(2026, 10, 21, 12, 34, 56, 789, TimeSpan.Zero);
        var msg = MessageFactory.Create(Id12Digits, Guid.NewGuid(), 8, createdAt);

        using var doc = JsonDocument.Parse(msg.Body);
        doc.RootElement.GetProperty("timestamp").GetString().ShouldBe("2026-10-21T12:34:56.789Z");
        msg.CreatedAt.ShouldBe(createdAt);
    }

    [Fact]
    public void Create_ZeroPayload_IsValidJson()
    {
        var msg = MessageFactory.Create(Id12Digits, Guid.NewGuid(), 0);
        using var doc = JsonDocument.Parse(msg.Body);
        doc.RootElement.GetProperty("payload").GetString().ShouldBe(string.Empty);
        msg.BodyBytes.ShouldBeGreaterThan(0); // остаётся конверт id/runId/eventType/timestamp
    }

    [Fact]
    public void EstimateBodyBytes_MatchesCreated()
    {
        var runId = Guid.NewGuid();
        var estimate = MessageFactory.EstimateBodyBytes(runId, 300);
        var created = MessageFactory.Create(Id12Digits, runId, 300).BodyBytes;
        estimate.ShouldBe(created); // длина payload фиксирована, ширина id = оценка → точна
    }

    [Fact]
    public void CreateBatch_SumsBytesAndKeepsIds()
    {
        var runId = Guid.NewGuid();
        var ids = Enumerable.Range(0, 50).Select(i => (long)(Id12Digits + i)).ToArray();
        var (messages, total) = MessageFactory.CreateBatch(ids, runId, 128);

        messages.Length.ShouldBe(50);
        total.ShouldBe(messages.Sum(m => (long)m.BodyBytes));
        messages.Select(m => m.Id).ShouldBe(ids); // id — строго зарезервированные значения
    }
}
