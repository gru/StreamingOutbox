using Shouldly;
using Writer.Generation;
using Xunit;

namespace Writer.UnitTests;

public class PercentilesTests
{
    [Fact]
    public void P50P95P99_EmptyInput_ReturnsZeros()
    {
        var (p50, p95, p99) = Percentiles.P50P95P99(Array.Empty<double>());
        p50.ShouldBe(0);
        p95.ShouldBe(0);
        p99.ShouldBe(0);
    }

    [Fact]
    public void P50P95P99_NearestRankOnSorted()
    {
        // 1..100, nearest-rank: ceil(p/100*n)
        var samples = Enumerable.Range(1, 100).Select(i => (double)i).ToArray();
        var (p50, p95, p99) = Percentiles.P50P95P99(samples);
        p50.ShouldBe(50);
        p95.ShouldBe(95);
        p99.ShouldBe(99);
    }

    [Fact]
    public void P50P95P99_SingleSample()
    {
        var (p50, p95, p99) = Percentiles.P50P95P99(new[] { 42.5 });
        p50.ShouldBe(42.5);
        p95.ShouldBe(42.5);
        p99.ShouldBe(42.5);
    }

    [Fact]
    public void P50P95P99_UnsortedInput_IsSortedInternally()
    {
        var samples = new[] { 9.0, 1.0, 5.0, 3.0, 7.0 };
        var (p50, _, p99) = Percentiles.P50P95P99(samples);
        p50.ShouldBe(5);
        p99.ShouldBe(9);
    }
}
