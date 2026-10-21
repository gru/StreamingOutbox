using System.Text;
using System.Text.Json;
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
        Assert.Equal(Id12Digits, root.GetProperty("id").GetInt64());
        Assert.Equal("TestEvent", root.GetProperty("eventType").GetString());
        Assert.Equal(runId.ToString(), root.GetProperty("runId").GetString());
        Assert.Equal(512, root.GetProperty("payload").GetString()!.Length);
        Assert.Equal(MessageFactory.Topic, msg.Topic); // 'outbox.events' — единственный разрешённый topic
        Assert.Equal(msg.BodyBytes, Encoding.UTF8.GetByteCount(Encoding.UTF8.GetString(msg.Body)));
    }

    [Fact]
    public void Create_KeyIsUtf8OfDecimalId_HeadersEmptyObject()
    {
        var msg = MessageFactory.Create(Id12Digits, Guid.NewGuid(), 8);

        Assert.Equal(Encoding.UTF8.GetBytes(Id12Digits.ToString()), msg.Key);
        Assert.Equal("{}", msg.Headers);
    }

    [Fact]
    public void Create_TimestampIsClientUtc_Milliseconds()
    {
        var createdAt = new DateTimeOffset(2026, 10, 21, 12, 34, 56, 789, TimeSpan.Zero);
        var msg = MessageFactory.Create(Id12Digits, Guid.NewGuid(), 8, createdAt);

        using var doc = JsonDocument.Parse(msg.Body);
        Assert.Equal("2026-10-21T12:34:56.789Z", doc.RootElement.GetProperty("timestamp").GetString());
        Assert.Equal(createdAt, msg.CreatedAt);
    }

    [Fact]
    public void Create_ZeroPayload_IsValidJson()
    {
        var msg = MessageFactory.Create(Id12Digits, Guid.NewGuid(), 0);
        using var doc = JsonDocument.Parse(msg.Body);
        Assert.Equal(string.Empty, doc.RootElement.GetProperty("payload").GetString());
        Assert.True(msg.BodyBytes > 0); // остаётся конверт id/runId/eventType/timestamp
    }

    [Fact]
    public void EstimateBodyBytes_MatchesCreated()
    {
        var runId = Guid.NewGuid();
        var estimate = MessageFactory.EstimateBodyBytes(runId, 300);
        var created = MessageFactory.Create(Id12Digits, runId, 300).BodyBytes;
        Assert.Equal(created, estimate); // длина payload фиксирована, ширина id = оценка → точна
    }

    [Fact]
    public void CreateBatch_SumsBytesAndKeepsIds()
    {
        var runId = Guid.NewGuid();
        var ids = Enumerable.Range(0, 50).Select(i => (long)(Id12Digits + i)).ToArray();
        var (messages, total) = MessageFactory.CreateBatch(ids, runId, 128);

        Assert.Equal(50, messages.Length);
        Assert.Equal(messages.Sum(m => (long)m.BodyBytes), total);
        Assert.Equal(ids, messages.Select(m => m.Id)); // id — строго зарезервированные значения
    }
}
