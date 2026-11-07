using Writer.Generation;

namespace Writer.Contracts;

/// <summary>
/// Живой снимок состояния генерации (GET /api/generation/state).
/// </summary>
public sealed class RunStateSnapshot
{
    /// <summary>
    /// Текущее состояние (константы StateIdle/StateRunning/... движка).
    /// </summary>
    public string State { get; set; } = GenerationEngine.StateIdle;

    /// <summary>
    /// Идентификатор текущего/последнего прогона; null — прогонов не было.
    /// </summary>
    public Guid? RunId { get; set; }

    /// <summary>
    /// Момент старта текущего/последнего прогона (UTC); null — прогонов не было.
    /// </summary>
    public DateTimeOffset? StartedAt { get; set; }

    /// <summary>
    /// Прошло с момента старта текущего/последнего прогона.
    /// </summary>
    public double? ElapsedSeconds { get; set; }

    /// <summary>
    /// Профиль текущего/последнего прогона; null — прогонов не было.
    /// </summary>
    public LoadProfile? Profile { get; set; }

    /// <summary>
    /// Диспетчеризовано сообщений.
    /// </summary>
    public long Dispatched { get; set; }

    /// <summary>
    /// Зафиксировано commit'ом.
    /// </summary>
    public long Written { get; set; }

    /// <summary>
    /// Записано байт body.
    /// </summary>
    public long BytesWritten { get; set; }

    /// <summary>
    /// Неоднозначные commit'ы (исход неизвестен на момент снимка).
    /// </summary>
    public long Ambiguous { get; set; }

    /// <summary>
    /// Неудачные сообщения.
    /// </summary>
    public long Failed { get; set; }

    /// <summary>
    /// Фактическая скорость, сообщений/с.
    /// </summary>
    public double ActualRatePerSecond { get; set; }

    /// <summary>
    /// Батчей в работе прямо сейчас.
    /// </summary>
    public int InFlight { get; set; }

    /// <summary>
    /// Последняя ошибка прогона.
    /// </summary>
    public string? LastError { get; set; }

    /// <summary>
    /// Причина завершения последнего прогона.
    /// </summary>
    public string? LastStopReason { get; set; }
}
