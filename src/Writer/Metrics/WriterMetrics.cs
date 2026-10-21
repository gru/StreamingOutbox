using Prometheus;

namespace Writer.Metrics;

/// <summary>
/// Метрики Writer — prometheus-net (pull /metrics, dependencies.md §2).
/// Кардинальность фиксированная: message id не используется как label.
/// ВНИМАНИЕ: Prometheus.Metrics квалифицируется полностью — внутри namespace
/// Writer.Metrics имя Metrics резолвится в сам namespace.
/// </summary>
public sealed class WriterMetrics
{
    public Counter MessagesWritten { get; }
    public Counter BytesWritten { get; }
    public Histogram CommitDurationMs { get; }
    public Histogram BatchDurationMs { get; }
    public Counter WriteErrors { get; }
    public Counter AmbiguousCommits { get; }
    public Counter Reconnects { get; }
    public Gauge GenerationActive { get; }

    public WriterMetrics()
    {
        MessagesWritten = Prometheus.Metrics.CreateCounter(
            "writer_messages_written_total",
            "Сообщения, зафиксированные commit'ом в outbox.messages");
        BytesWritten = Prometheus.Metrics.CreateCounter(
            "writer_bytes_written_total",
            "UTF-8 байты body записанных сообщений");
        CommitDurationMs = Prometheus.Metrics.CreateHistogram(
            "writer_commit_duration_milliseconds",
            "Длительность Npgsql CommitAsync транзакционного батча (мс)");
        BatchDurationMs = Prometheus.Metrics.CreateHistogram(
            "writer_batch_duration_milliseconds",
            "Полная длительность записи батча, включая ретраи (мс)");
        WriteErrors = Prometheus.Metrics.CreateCounter(
            "writer_write_errors_total",
            "Ошибки записи: reason=fatal|ambiguous", "reason");
        AmbiguousCommits = Prometheus.Metrics.CreateCounter(
            "writer_ambiguous_commits_total",
            "Батчи, чей commit остался незавершённым (остановка во время retry)");
        Reconnects = Prometheus.Metrics.CreateCounter(
            "writer_reconnects_total",
            "Повторные попытки записи после транзиентных сбоев соединения");
        GenerationActive = Prometheus.Metrics.CreateGauge(
            "writer_generation_active",
            "1 — генерация выполняется, 0 — idle");
    }

    public void AddWriteError(string reason) =>
        WriteErrors.WithLabels(reason).Inc();
}
