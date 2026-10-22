using Shouldly;
using Writer.Configuration;
using Writer.Contracts;
using Writer.Generation;
using Xunit;

namespace Writer.UnitTests;

public class LoadProfileTests
{
    private static WriterOptions Options() => new()
    {
        Defaults = new()
        {
            MessageSizeBytes = 1024,
            Parallelism = 4,
            BatchSize = 100,
        },
    };

    [Fact]
    public void Resolve_WithoutRequest_UsesDefaultsAndRequiresTarget()
    {
        var errors = new List<string>();
        var p = LoadProfile.Resolve(Options(), null, errors);

        p.ShouldBeNull(); // ни цели, ни runUntilStopped
        errors.ShouldContain(e => e.Contains("цель завершения"));
    }

    [Fact]
    public void Resolve_DefaultsPlusRunUntilStopped_IsValid()
    {
        var errors = new List<string>();
        var p = LoadProfile.Resolve(Options(), new RunSpec { RunUntilStopped = true }, errors);

        p.ShouldNotBeNull();
        p.MessageSizeBytes.ShouldBe(1024);
        p.Parallelism.ShouldBe(4);
        p.BatchSize.ShouldBe(100);
        p.RatePerSecond.ShouldBeNull();
        p.RunUntilStopped.ShouldBeTrue();
        errors.ShouldBeEmpty();
    }

    [Fact]
    public void Resolve_RequestOverridesDefaults()
    {
        var errors = new List<string>();
        var p = LoadProfile.Resolve(Options(), new RunSpec
        {
            TotalMessages = 1234,
            MessageSizeBytes = 256,
            RatePerSecond = 500,
            Parallelism = 8,
            BatchSize = 64,
            RunId = Guid.NewGuid(),
        }, errors);

        p.ShouldNotBeNull();
        p!.TotalMessages.ShouldBe(1234);
        p.MessageSizeBytes.ShouldBe(256);
        p.RatePerSecond.ShouldBe(500);
        p.Parallelism.ShouldBe(8);
        p.BatchSize.ShouldBe(64);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(OutboxWriter.MaxBatchSize + 1)]
    public void Resolve_BatchSizeOutOfRange_Fails(int batchSize)
    {
        var errors = new List<string>();
        var p = LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 10, BatchSize = batchSize }, errors);
        p.ShouldBeNull();
        errors.ShouldContain(e => e.Contains("batchSize"));
    }

    [Fact]
    public void Resolve_MaxBatchSize_IsValid()
    {
        var errors = new List<string>();
        var p = LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 10_000, BatchSize = OutboxWriter.MaxBatchSize }, errors);
        p.ShouldNotBeNull();
    }

    [Fact]
    public void Resolve_OutOfRangeNumericTargets_Fail()
    {
        var errors = new List<string>();
        LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 0 }, errors).ShouldBeNull();
        LoadProfile.Resolve(Options(), new RunSpec { TotalPayloadBytes = 0 }, errors).ShouldBeNull();
        LoadProfile.Resolve(Options(), new RunSpec { DurationSeconds = 0 }, errors).ShouldBeNull();
        LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 5, Parallelism = 0 }, errors).ShouldBeNull();
        LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 5, Parallelism = 65 }, errors).ShouldBeNull();
        LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 5, RatePerSecond = 0 }, errors).ShouldBeNull();
        LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 5, MessageSizeBytes = 0 }, errors).ShouldBeNull();
    }
}
