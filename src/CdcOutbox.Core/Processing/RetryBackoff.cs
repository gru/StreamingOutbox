namespace CdcOutbox.Core.Processing;

/// <summary>
/// Пауза повторной попытки: удваивается от базовой после каждой неудачи
/// до предельной, сбрасывается к базовой после успеха.
/// </summary>
/// <param name="baseDelay">Базовая пауза первой повторной попытки.</param>
/// <param name="maxDelay">Предельная пауза.</param>
internal sealed class RetryBackoff(TimeSpan baseDelay, TimeSpan maxDelay)
{
    private int _attempt;

    /// <summary>
    /// Пауза следующей повторной попытки.
    /// </summary>
    public TimeSpan NextDelay()
    {
        var multiplier = 1 << Math.Min(_attempt, 30);
        _attempt++;
        return TimeSpan.FromTicks(Math.Min(baseDelay.Ticks * multiplier, maxDelay.Ticks));
    }

    /// <summary>
    /// Сбросить счётчик неудач: следующая пауза снова базовая.
    /// </summary>
    public void Reset() => _attempt = 0;
}
