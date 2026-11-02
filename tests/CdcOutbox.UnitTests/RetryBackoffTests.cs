using CdcOutbox.Core.Processing;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class RetryBackoffTests
{
    [Fact]
    public void NextDelay_DoublesFromBaseUntilCap()
    {
        var backoff = new RetryBackoff(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(60));

        backoff.NextDelay().ShouldBe(TimeSpan.FromSeconds(5));
        backoff.NextDelay().ShouldBe(TimeSpan.FromSeconds(10));
        backoff.NextDelay().ShouldBe(TimeSpan.FromSeconds(20));
        backoff.NextDelay().ShouldBe(TimeSpan.FromSeconds(40));
        backoff.NextDelay().ShouldBe(TimeSpan.FromSeconds(60));
        backoff.NextDelay().ShouldBe(TimeSpan.FromSeconds(60));
    }

    [Fact]
    public void NextDelay_BaseAboveCap_StaysAtCap()
    {
        var backoff = new RetryBackoff(TimeSpan.FromSeconds(90), TimeSpan.FromSeconds(60));

        backoff.NextDelay().ShouldBe(TimeSpan.FromSeconds(60));
    }

    [Fact]
    public void Reset_ReturnsToBaseDelay()
    {
        var backoff = new RetryBackoff(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(60));
        backoff.NextDelay();
        backoff.NextDelay();
        backoff.NextDelay();

        backoff.Reset();

        backoff.NextDelay().ShouldBe(TimeSpan.FromSeconds(5));
    }
}
