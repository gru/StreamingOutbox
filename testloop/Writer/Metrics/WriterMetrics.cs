using Prometheus;

namespace Writer.Metrics;

/// <summary>
/// Метрики Writer — prometheus-net (pull /metrics); кардинальность фиксированная,
/// регистрация единственная в default-registry процесса (фиксируется тестом имён).
/// Особенности — docs/writer.md.
/// </summary>
public sealed class WriterMetrics
{
    /// <summary>
    /// Сообщения, зафиксированные commit'ом (writer_messages_written_total).
    /// </summary>
    public Counter MessagesWritten { get; } = Prometheus.Metrics.CreateCounter(
        "writer_messages_written_total",
        "Сообщения, зафиксированные commit'ом в outbox.messages");

    /// <summary>
    /// UTF-8 байты body записанных сообщений (writer_bytes_written_total).
    /// </summary>
    public Counter BytesWritten { get; } = Prometheus.Metrics.CreateCounter(
        "writer_bytes_written_total",
        "UTF-8 байты body записанных сообщений");

    /// <summary>
    /// Длительность Npgsql CommitAsync транзакционного батча, мс
    /// (writer_commit_duration_milliseconds).
    /// </summary>
    public Histogram CommitDurationMs { get; } = Prometheus.Metrics.CreateHistogram(
        "writer_commit_duration_milliseconds",
        "Длительность Npgsql CommitAsync транзакционного батча (мс)");

    /// <summary>
    /// Полная длительность записи батча, включая ретраи, мс
    /// (writer_batch_duration_milliseconds).
    /// </summary>
    public Histogram BatchDurationMs { get; } = Prometheus.Metrics.CreateHistogram(
        "writer_batch_duration_milliseconds",
        "Полная длительность записи батча, включая ретраи (мс)");

    /// <summary>
    /// Ошибки записи с label reason=fatal|ambiguous (writer_write_errors_total).
    /// </summary>
    public Counter WriteErrors { get; } = Prometheus.Metrics.CreateCounter(
        "writer_write_errors_total",
        "Ошибки записи: reason=fatal|ambiguous", "reason");

    /// <summary>
    /// Батчи, чей commit остался незавершённым (writer_ambiguous_commits_total).
    /// </summary>
    public Counter AmbiguousCommits { get; } = Prometheus.Metrics.CreateCounter(
        "writer_ambiguous_commits_total",
        "Батчи, чей commit остался незавершённым (остановка во время retry)");

    /// <summary>
    /// Повторные попытки записи после транзиентных сбоев (writer_reconnects_total).
    /// </summary>
    public Counter Reconnects { get; } = Prometheus.Metrics.CreateCounter(
        "writer_reconnects_total",
        "Повторные попытки записи после транзиентных сбоев соединения");

    /// <summary>
    /// 1 — генерация выполняется, 0 — idle (writer_generation_active).
    /// </summary>
    public Gauge GenerationActive { get; } = Prometheus.Metrics.CreateGauge(
        "writer_generation_active",
        "1 — генерация выполняется, 0 — idle");

    /// <summary>
    /// Инкремент ошибки записи по причине (fatal|ambiguous).
    /// </summary>
    public void AddWriteError(string reason) =>
        WriteErrors.WithLabels(reason).Inc();
}
