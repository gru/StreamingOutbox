using Shouldly;
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

        metrics.MessagesWritten.Name.ShouldBe("writer_messages_written_total");
        metrics.BytesWritten.Name.ShouldBe("writer_bytes_written_total");
        metrics.CommitDurationMs.Name.ShouldBe("writer_commit_duration_milliseconds");
        metrics.BatchDurationMs.Name.ShouldBe("writer_batch_duration_milliseconds");
        metrics.WriteErrors.Name.ShouldBe("writer_write_errors_total");
        metrics.AmbiguousCommits.Name.ShouldBe("writer_ambiguous_commits_total");
        metrics.Reconnects.Name.ShouldBe("writer_reconnects_total");
        metrics.GenerationActive.Name.ShouldBe("writer_generation_active");

        // контр с label доступен и инкрементится
        metrics.AddWriteError("fatal");
        metrics.GenerationActive.Set(1);
        metrics.GenerationActive.Value.ShouldBe(1);
    }
}
