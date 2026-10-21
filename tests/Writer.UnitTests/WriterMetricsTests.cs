using Writer.Metrics;

using Xunit;

namespace Writer.UnitTests;

/// <summary>
/// Фиксация фактических имён метрик prometheus-net (экспозиция /metrics):
/// счётчики именуются с суффиксом _total (классический и OpenMetrics форматы
/// дают одинаковые серии), гистограммы — базовое имя без суффиксов.
/// </summary>
public class WriterMetricsTests
{
    [Fact]
    public void MetricNames_MatchDependenciesDoc()
    {
        var metrics = new WriterMetrics(); // единственная регистрация в default-registry процесса

        Assert.Equal("writer_messages_written_total", metrics.MessagesWritten.Name);
        Assert.Equal("writer_bytes_written_total", metrics.BytesWritten.Name);
        Assert.Equal("writer_commit_duration_milliseconds", metrics.CommitDurationMs.Name);
        Assert.Equal("writer_batch_duration_milliseconds", metrics.BatchDurationMs.Name);
        Assert.Equal("writer_write_errors_total", metrics.WriteErrors.Name);
        Assert.Equal("writer_ambiguous_commits_total", metrics.AmbiguousCommits.Name);
        Assert.Equal("writer_reconnects_total", metrics.Reconnects.Name);
        Assert.Equal("writer_generation_active", metrics.GenerationActive.Name);

        // контр с label доступен и инкрементится
        metrics.AddWriteError("fatal");
        metrics.GenerationActive.Set(1);
        Assert.Equal(1, metrics.GenerationActive.Value);
    }
}
