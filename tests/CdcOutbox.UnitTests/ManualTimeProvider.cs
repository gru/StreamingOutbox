namespace CdcOutbox.UnitTests;

internal sealed class ManualTimeProvider(DateTimeOffset initialUtc) : TimeProvider
{
    private readonly object _gate = new();
    private readonly List<ManualTimer> _timers = new();
    private DateTimeOffset _utcNow = initialUtc.ToUniversalTime();
    private int _createdTimerCount;

    public int CreatedTimerCount
    {
        get
        {
            lock (_gate)
            {
                return _createdTimerCount;
            }
        }
    }

    public override DateTimeOffset GetUtcNow()
    {
        lock (_gate)
        {
            return _utcNow;
        }
    }

    public void Advance(TimeSpan delta)
    {
        List<ManualTimer> due;
        lock (_gate)
        {
            _utcNow += delta;
            due = _timers.Where(t => t.IsDue(_utcNow)).ToList();
        }

        foreach (var timer in due)
        {
            timer.Fire(_utcNow);
        }
    }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new ManualTimer(this, callback, state, dueTime, period);
        lock (_gate)
        {
            _createdTimerCount++;
            _timers.Add(timer);
            if (dueTime == TimeSpan.Zero)
            {
                timer.Fire(_utcNow);
            }
        }

        return timer;
    }

    private void Remove(ManualTimer timer)
    {
        lock (_gate)
        {
            _timers.Remove(timer);
        }
    }

    private sealed class ManualTimer : ITimer
    {
        private readonly ManualTimeProvider _owner;
        private readonly TimerCallback _callback;
        private readonly object? _state;
        private readonly object _sync = new();
        private DateTimeOffset? _nextDue;
        private TimeSpan _period;
        private bool _disposed;

        public ManualTimer(
            ManualTimeProvider owner,
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
        {
            _owner = owner;
            _callback = callback;
            _state = state;
            _period = period;
            _nextDue = dueTime == Timeout.InfiniteTimeSpan ? null : owner.GetUtcNow() + dueTime;
        }

        public bool IsDue(DateTimeOffset now)
        {
            lock (_sync)
            {
                return !_disposed && _nextDue is { } due && now >= due;
            }
        }

        public void Fire(DateTimeOffset now)
        {
            lock (_sync)
            {
                if (_disposed)
                {
                    return;
                }

                _nextDue = _period == Timeout.InfiniteTimeSpan ? null : now + _period;
            }

            _callback(_state);
        }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (_sync)
            {
                if (_disposed)
                {
                    return false;
                }

                _period = period;
                _nextDue = dueTime == Timeout.InfiniteTimeSpan ? null : _owner.GetUtcNow() + dueTime;
                return true;
            }
        }

        public void Dispose()
        {
            lock (_sync)
            {
                _disposed = true;
                _nextDue = null;
            }

            _owner.Remove(this);
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
