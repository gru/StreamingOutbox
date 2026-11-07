using System.Text.Json.Serialization;

namespace Writer.Contracts;

/// <summary>
/// Итоги завершённого прогона (GET /api/generation/results).
/// </summary>
public sealed class RunResults
{
    /// <summary>
    /// Идентификатор прогона.
    /// </summary>
    public Guid RunId { get; set; }

    /// <summary>
    /// Разрешённый профиль прогона.
    /// </summary>
    public LoadProfile? Profile { get; set; }

    /// <summary>
    /// completed | failed | stopped
    /// </summary>
    public string State { get; set; } = "";

    /// <summary>
    /// TargetMessages | TargetPayloadBytes | TargetDuration | UserStop | HostShutdown | FatalError
    /// </summary>
    public string StopReason { get; set; } = "";

    /// <summary>
    /// Момент старта (UTC).
    /// </summary>
    public DateTimeOffset StartedAt { get; set; }

    /// <summary>
    /// Момент завершения (UTC).
    /// </summary>
    public DateTimeOffset FinishedAt { get; set; }

    /// <summary>
    /// Длительность прогона, с.
    /// </summary>
    public double DurationSeconds { get; set; }

    /// <summary>
    /// Диспетчеризовано сообщений (взятые в работу).
    /// </summary>
    public long DispatchedMessages { get; set; }

    /// <summary>
    /// Зафиксированные commit'ом сообщения (включая доказанные ретраем, см. AlreadyCommittedDuplicates).
    /// </summary>
    public long WrittenMessages { get; set; }

    /// <summary>
    /// Неоднозначные commit'ы, разрешённые повтором (unique violation по PK).
    /// </summary>
    public long AlreadyCommittedDuplicates { get; set; }

    /// <summary>
    /// Сообщения с незавершённым исходом commit (остановка во время retry после сбоя соединения).
    /// </summary>
    public long AmbiguousUnresolved { get; set; }

    /// <summary>
    /// Сообщения, чья запись завершилась фатальной ошибкой.
    /// </summary>
    public long FailedMessages { get; set; }

    /// <summary>
    /// Суммарный размер записанных body, байт.
    /// </summary>
    public long BytesWritten { get; set; }

    /// <summary>
    /// Выполнено батчей.
    /// </summary>
    public int Batches { get; set; }

    /// <summary>
    /// Выполнено commit'ов.
    /// </summary>
    public int Commits { get; set; }

    /// <summary>
    /// Повторных попыток после транзиентных сбоев.
    /// </summary>
    public int Reconnects { get; set; }

    /// <summary>
    /// Средняя длительность commit, мс.
    /// </summary>
    public double CommitMeanMs { get; set; }

    /// <summary>
    /// P50 длительности commit, мс.
    /// </summary>
    public double CommitP50Ms { get; set; }

    /// <summary>
    /// P95 длительности commit, мс.
    /// </summary>
    public double CommitP95Ms { get; set; }

    /// <summary>
    /// P99 длительности commit, мс.
    /// </summary>
    public double CommitP99Ms { get; set; }

    /// <summary>
    /// Средняя длительность батча (с ретраями), мс.
    /// </summary>
    public double BatchMeanMs { get; set; }

    /// <summary>
    /// P50 длительности батча, мс.
    /// </summary>
    public double BatchP50Ms { get; set; }

    /// <summary>
    /// P95 длительности батча, мс.
    /// </summary>
    public double BatchP95Ms { get; set; }

    /// <summary>
    /// P99 длительности батча, мс.
    /// </summary>
    public double BatchP99Ms { get; set; }

    /// <summary>
    /// Фактическая скорость, сообщений/с.
    /// </summary>
    public double ActualRatePerSecond { get; set; }

    /// <summary>
    /// Фактическая скорость записи, байт/с.
    /// </summary>
    public double ActualBytesPerSecond { get; set; }

    /// <summary>
    /// Первые 20 ошибок прогона (полный счёт — в метриках).
    /// </summary>
    public List<string> Errors { get; set; } = new();

    /// <summary>
    /// id записанных сообщений (в JSON не отдаётся; основа /results/ids).
    /// </summary>
    [JsonIgnore] public List<long> WrittenIds { get; } = new();

    /// <summary>
    /// id неоднозначно-завершённых сообщений (в JSON не отдаётся).
    /// </summary>
    [JsonIgnore] public List<long> AmbiguousIds { get; } = new();

    /// <summary>
    /// id неудачных сообщений (в JSON не отдаётся).
    /// </summary>
    [JsonIgnore] public List<long> FailedIds { get; } = new();
}
