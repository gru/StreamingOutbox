using System.Diagnostics;

namespace CdcOutbox.Core.Processing;

/// <summary>
/// Таймер простоя потока: отслеживает время с последней активности
/// и решает, когда выдать очередное idle-предупреждение — не чаще
/// одного раза за интервал.
/// </summary>
internal sealed class IdleActivityTracker
{
    private readonly object _gate = new();
    private long _lastActivity = Stopwatch.GetTimestamp();
    private long _lastWarning;

    /// <summary>
    /// Фиксирует активность (новое outbox-событие) и сбрасывает таймер простоя.
    /// </summary>
    public void MarkActivity()
    {
        lock (_gate)
        {
            _lastActivity = Stopwatch.GetTimestamp();
            _lastWarning = 0;
        }
    }

    /// <summary>
    /// Возвращает фактическое время простоя, если пора предупредить, иначе null.
    /// </summary>
    public TimeSpan? TryBeginWarning(TimeSpan idleTimeout)
    {
        lock (_gate)
        {
            var now = Stopwatch.GetTimestamp();
            var idle = Stopwatch.GetElapsedTime(_lastActivity, now);
            if (idle < idleTimeout)
            {
                return null;
            }

            if (_lastWarning != 0 && Stopwatch.GetElapsedTime(_lastWarning, now) < idleTimeout)
            {
                return null;
            }

            _lastWarning = now;
            return idle;
        }
    }
}
