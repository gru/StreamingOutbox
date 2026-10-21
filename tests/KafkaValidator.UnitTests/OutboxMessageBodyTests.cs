using KafkaValidator.Validation;
using Xunit;

namespace KafkaValidator.UnitTests;

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
