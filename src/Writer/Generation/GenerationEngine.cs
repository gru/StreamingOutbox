using Writer.Contracts;
using Writer.Metrics;

namespace Writer.Generation;

/// <summary>
/// Оркестратор прогона нагрузки; стартует в Idle, состояние — in-memory. Особенности — docs/writer.md.
/// </summary>
/// <param name="db">Писатель батчей.</param>
/// <param name="metrics">Метрики.</param>
/// <param name="logger">Лог.</param>
public sealed class GenerationEngine(OutboxWriter db, WriterMetrics metrics, ILogger<GenerationEngine> logger) : IHostedService
{
    /// <summary>
    /// Прогонов не было (или после рестарта процесса).
    /// </summary>
    public const string StateIdle = "idle";

    /// <summary>
    /// Прогон выполняется.
    /// </summary>
    public const string StateRunning = "running";

    /// <summary>
    /// Идёт мягкая остановка (дожидаемся in-flight батчей).
    /// </summary>
    public const string StateStopping = "stopping";

    /// <summary>
    /// Прогон завершился достижением цели.
    /// </summary>
    public const string StateCompleted = "completed";

    /// <summary>
    /// Прогон упал (фатальная ошибка).
    /// </summary>
    public const string StateFailed = "failed";

    /// <summary>
    /// Прогон остановлен явно (POST stop).
    /// </summary>
    public const string StateStopped = "stopped";

    private readonly object _gate = new();
    private string _state = StateIdle;
    private RunContext? _current;
    private RunResults? _lastResults;
    private string? _lastStopReason;

    /// <summary>
    /// Текущее состояние движка (константы State*); потокобезопасно.
    /// </summary>
    public string State { get { lock (_gate) return _state; } }

    /// <summary>
    /// true в состояниях running/stopping.
    /// </summary>
    public bool IsRunning => State is StateRunning or StateStopping;

    /// <summary>
    /// Итоги последнего завершённого прогона; null — завершённых прогонов нет.
    /// </summary>
    public RunResults? LastResults { get { lock (_gate) return _lastResults; } }

    /// <summary>
    /// IHostedService: движок пассивен до HTTP start, фоновой работы при старте хоста нет.
    /// </summary>
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Остановка хоста: мягкая остановка (до 20 с), затем жёсткая отмена остатка (до 5 с).
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await StopAsync(TimeSpan.FromSeconds(20));
        RunContext? ctx;
        lock (_gate) ctx = _current;
        if (ctx is not null && !ctx.RunTask.IsCompleted)
        {
            try { ctx.HardCts.Cancel(); } catch (ObjectDisposedException) { }
            try { await ctx.RunTask.WaitAsync(TimeSpan.FromSeconds(5)); } catch { }
        }
    }

    /// <summary>
    /// Запускает прогон профилем; false + причина — генерация уже выполняется.
    /// </summary>
    public (bool Success, string Error) Start(LoadProfile profile)
    {
        lock (_gate)
        {
            if (_state is StateRunning or StateStopping)
                return (false, $"генерация уже выполняется (state={_state}, runId={_current?.Profile.RunId})");
            var ctx = new RunContext
            {
                Profile = profile,
                StartedAt = DateTimeOffset.UtcNow,
                HardCts = new CancellationTokenSource(),
                StopCts = new CancellationTokenSource(),
                Sem = new SemaphoreSlim(profile.Parallelism, profile.Parallelism),
            };
            _current = ctx;
            _state = StateRunning;
            ctx.RunTask = Task.Run(() => RunAsync(ctx));
        }
        metrics.GenerationActive.Set(1);
        return (true, string.Empty);
    }

    /// <summary>
    /// Мягкая остановка: прекратить диспетчеризацию, дождаться in-flight батчей.
    /// </summary>
    public async Task<RunResults?> StopAsync(TimeSpan waitTimeout)
    {
        RunContext? ctx;
        lock (_gate)
        {
            if (_state is not (StateRunning or StateStopping)) return _lastResults;
            _state = StateStopping;
            ctx = _current;
        }
        if (ctx is not null)
        {
            ctx.StopCts.Cancel();
            if (!ctx.RunTask.IsCompleted)
            {
                var done = await Task.WhenAny(ctx.RunTask, Task.Delay(waitTimeout));
                if (done != ctx.RunTask)
                {
                    logger.LogWarning("Прогон {RunId}: таймаут мягкой остановки ({Sec:F0}s), прогон ещё завершается",
                        ctx.Profile.RunId, waitTimeout.TotalSeconds);
                }
            }
        }
        return LastResults;
    }

    /// <summary>
    /// Снимок состояния для GET /api/generation/state; потокобезопасно.
    /// </summary>
    public RunStateSnapshot GetState()
    {
        lock (_gate)
        {
            var snap = new RunStateSnapshot
            {
                State = _state,
                LastStopReason = _lastStopReason,
                RunId = _current?.Profile.RunId,
                Profile = _current?.Profile,
            };
            if (_current is { } ctx)
            {
                lock (ctx.Sync)
                {
                    var elapsed = (DateTimeOffset.UtcNow - ctx.StartedAt).TotalSeconds;
                    snap.StartedAt = ctx.StartedAt;
                    snap.ElapsedSeconds = elapsed;
                    snap.Dispatched = ctx.Dispatched;
                    snap.Written = ctx.Written;
                    snap.BytesWritten = ctx.Bytes;
                    snap.Ambiguous = ctx.Ambiguous;
                    snap.Failed = ctx.Failed;
                    snap.InFlight = ctx.Tasks.Count(t => !t.IsCompleted);
                    snap.LastError = ctx.FatalError ?? (ctx.ErrorMessages.Count > 0 ? ctx.ErrorMessages[^1] : null);
                    snap.ActualRatePerSecond = ctx.Written / Math.Max(elapsed, 0.001);
                }
            }
            else if (_lastResults is { } r)
            {
                snap.RunId = r.RunId;
                snap.StartedAt = r.StartedAt;
                snap.ElapsedSeconds = r.DurationSeconds;
                snap.Dispatched = r.DispatchedMessages;
                snap.Written = r.WrittenMessages;
                snap.BytesWritten = r.BytesWritten;
                snap.Ambiguous = r.AmbiguousUnresolved;
                snap.Failed = r.FailedMessages;
                snap.ActualRatePerSecond = r.ActualRatePerSecond;
            }
            return snap;
        }
    }

    /// <summary>
    /// Ожидаемый набор id последнего завершённого прогона (для KafkaValidator);
    /// null — завершённых прогонов нет.
    /// </summary>
    public RunIdsDto? GetLastIds()
    {
        var r = LastResults;
        return r is null
            ? null
            : new RunIdsDto(r.RunId, r.WrittenIds.Count, r.AmbiguousIds.Count, r.FailedIds.Count,
                r.WrittenIds, r.AmbiguousIds, r.FailedIds);
    }

    private async Task RunAsync(RunContext ctx)
    {
        var p = ctx.Profile;
        logger.LogInformation(
            "Прогон {RunId} стартовал: rate={Rate} msg/s batch={Batch} par={Par} size={Size}B цели: count={Count} bytes={Bytes}B dur={Dur}s",
            p.RunId, p.RatePerSecond?.ToString() ?? "unlimited", p.BatchSize, p.Parallelism,
            p.MessageSizeBytes, p.TotalMessages?.ToString() ?? "-", p.TotalPayloadBytes?.ToString() ?? "-",
            p.DurationSeconds?.ToString() ?? "-");

        var avgBodyBytes = MessageFactory.EstimateBodyBytes(p.RunId, p.MessageSizeBytes);
        try
        {
            await DispatchLoopAsync(ctx, avgBodyBytes);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogError(ex, "Прогон {RunId}: сбой диспетчера", p.RunId);
            lock (ctx.Sync)
            {
                ctx.Fatal = true;
                ctx.FatalError ??= $"engine: {ex.Message}";
            }
        }

        try { await Task.WhenAll(ctx.Tasks.ToArray()); } catch { }
        FinalizeRun(ctx);
    }

    private async Task DispatchLoopAsync(RunContext ctx, int avgBodyBytes)
    {
        var p = ctx.Profile;
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ctx.StopCts.Token, ctx.HardCts.Token);
        var token = linked.Token;

        PeriodicTimer? timer = null;
        var firstTick = true;
        if (p.RatePerSecond is { } rate && rate > 0)
            timer = new PeriodicTimer(TimeSpan.FromSeconds((double)p.BatchSize / rate));

        try
        {
            while (!token.IsCancellationRequested && !ctx.Fatal)
            {
                if (RunTargets.StopDispatch(p, Interlocked.Read(ref ctx.Dispatched), Interlocked.Read(ref ctx.DispatchedBytes),
                        DateTimeOffset.UtcNow, ctx.StartedAt))
                {
                    break;
                }

                var size = RunTargets.NextBatchSize(p, Interlocked.Read(ref ctx.Dispatched),
                    Interlocked.Read(ref ctx.DispatchedBytes), avgBodyBytes);
                if (size <= 0) break;

                try { await ctx.Sem.WaitAsync(token); }
                catch (OperationCanceledException) { break; }

                if (token.IsCancellationRequested || ctx.Fatal ||
                    RunTargets.StopDispatch(p, Interlocked.Read(ref ctx.Dispatched), Interlocked.Read(ref ctx.DispatchedBytes),
                        DateTimeOffset.UtcNow, ctx.StartedAt))
                {
                    ctx.Sem.Release();
                    break;
                }

                Interlocked.Add(ref ctx.Dispatched, size);
                Interlocked.Add(ref ctx.DispatchedBytes, (long)avgBodyBytes * size);
                var task = Task.Run(() => ExecuteBatchAsync(ctx, size));
                lock (ctx.Sync) ctx.Tasks.Add(task);

                if (timer is not null)
                {
                    if (firstTick) { firstTick = false; continue; }
                    try { if (!await timer.WaitForNextTickAsync(token)) break; }
                    catch (OperationCanceledException) { break; }
                }
            }
        }
        finally
        {
            timer?.Dispose();
        }
    }

    private async Task ExecuteBatchAsync(RunContext ctx, int size)
    {
        GeneratedMessage[]? messages = null;
        long bodyBytes = 0;
        try
        {
            var ids = await db.ReserveIdsAsync(size, ctx.HardCts.Token, ctx.StopCts.Token);
            (messages, bodyBytes) = MessageFactory.CreateBatch(ids, ctx.Profile.RunId, ctx.Profile.MessageSizeBytes);

            var res = await db.WriteBatchAsync(messages, ctx.HardCts.Token, ctx.StopCts.Token);
            var reconnects = Math.Max(0, res.Attempts - 1);

            lock (ctx.Sync)
            {
                ctx.Batches++;
                ctx.Reconnects += reconnects;
                switch (res.Outcome)
                {
                    case BatchOutcome.Committed:
                    case BatchOutcome.AlreadyCommitted:
                        ctx.Written += messages.Length;
                        ctx.Bytes += bodyBytes;
                        ctx.Commits++;
                        if (res.Outcome == BatchOutcome.Committed) ctx.CommitMs.Add(res.CommitDurationMs);
                        ctx.BatchMs.Add(res.BatchDurationMs);
                        ctx.WrittenIds.AddRange(ids);
                        if (res.Outcome == BatchOutcome.AlreadyCommitted) ctx.AlreadyCommitted += messages.Length;
                        if (res.LastTransientError is not null)
                            ctx.ErrorMessages.Add($"{DateTimeOffset.UtcNow:O} transient→resolved: {Trunc(res.LastTransientError)}");
                        break;

                    case BatchOutcome.Cancelled:
                        ctx.Ambiguous += messages.Length;
                        ctx.AmbiguousIds.AddRange(ids);
                        ctx.ErrorMessages.Add($"{DateTimeOffset.UtcNow:O} AMBIGUOUS (остановка в retry): {Trunc(res.LastTransientError ?? "")}");
                        break;

                    case BatchOutcome.Fatal:
                        ctx.Failed += messages.Length;
                        ctx.FailedIds.AddRange(ids);
                        ctx.Fatal = true;
                        ctx.FatalError ??= res.FatalError ?? "unknown fatal";
                        ctx.ErrorMessages.Add($"{DateTimeOffset.UtcNow:O} FATAL: {Trunc(res.FatalError ?? "unknown")}");
                        break;
                }
                if (ctx.ErrorMessages.Count > 20) ctx.ErrorMessages.RemoveRange(0, ctx.ErrorMessages.Count - 20);
            }

            metrics.Reconnects.Inc(reconnects);
            switch (res.Outcome)
            {
                case BatchOutcome.Committed:
                case BatchOutcome.AlreadyCommitted:
                    metrics.MessagesWritten.Inc(messages.Length);
                    metrics.BytesWritten.Inc(bodyBytes);
                    metrics.BatchDurationMs.Observe(res.BatchDurationMs);
                    if (res.Outcome == BatchOutcome.Committed)
                        metrics.CommitDurationMs.Observe(res.CommitDurationMs);
                    break;
                case BatchOutcome.Cancelled:
                    metrics.AmbiguousCommits.Inc();
                    metrics.AddWriteError("ambiguous");
                    break;
                case BatchOutcome.Fatal:
                    metrics.AddWriteError("fatal");
                    break;
            }

            if (res.Outcome == BatchOutcome.Fatal)
            {
                logger.LogError("Прогон {RunId}: фатальная ошибка записи батча ({Count} сообщений): {Error}",
                    ctx.Profile.RunId, messages.Length, res.FatalError);
            }
        }
        catch (OperationCanceledException)
        {
            lock (ctx.Sync)
            {
                ctx.Ambiguous += size;
                ctx.ErrorMessages.Add($"{DateTimeOffset.UtcNow:O} AMBIGUOUS (отмена до вставки, при резервировании id)");
            }
            metrics.AmbiguousCommits.Inc();
            metrics.AddWriteError("ambiguous");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Прогон {RunId}: необработанное исключение в ExecuteBatchAsync", ctx.Profile.RunId);
            lock (ctx.Sync)
            {
                ctx.Fatal = true;
                ctx.FatalError ??= $"engine(batch): {ex.Message}";
                ctx.Failed += messages?.Length ?? size;
                if (messages is not null) ctx.FailedIds.AddRange(messages.Select(m => m.Id));
                ctx.ErrorMessages.Add($"{DateTimeOffset.UtcNow:O} FATAL(engine): {Trunc(ex.Message)}");
            }
            metrics.AddWriteError("fatal");
        }
        finally
        {
            ctx.Sem.Release();
        }
    }

    private void FinalizeRun(RunContext ctx)
    {
        var p = ctx.Profile;
        DateTimeOffset finished;
        long written, bytes, dispatched, alreadyCommitted, ambiguous, failed;
        int batches, commits, reconnects;
        List<long> writtenIds, ambiguousIds, failedIds;
        List<double> commitMs, batchMs;
        List<string> errors;
        string? fatalError;

        lock (ctx.Sync)
        {
            finished = DateTimeOffset.UtcNow;
            written = ctx.Written; bytes = ctx.Bytes; dispatched = ctx.Dispatched;
            alreadyCommitted = ctx.AlreadyCommitted; ambiguous = ctx.Ambiguous; failed = ctx.Failed;
            batches = ctx.Batches; commits = ctx.Commits; reconnects = ctx.Reconnects;
            writtenIds = ctx.WrittenIds; ambiguousIds = ctx.AmbiguousIds; failedIds = ctx.FailedIds;
            commitMs = ctx.CommitMs; batchMs = ctx.BatchMs; errors = new List<string>(ctx.ErrorMessages);
            fatalError = ctx.FatalError;
        }

        var durationSec = Math.Max((finished - ctx.StartedAt).TotalSeconds, 0.001);
        var target = RunTargets.ReachedTarget(p, written, bytes, finished, ctx.StartedAt);

        string state, reason;
        if (fatalError is not null)
        {
            state = StateFailed;
            reason = "FatalError";
        }
        else if (ctx.HardCts.IsCancellationRequested)
        {
            state = StateStopped;
            reason = "HostShutdown";
        }
        else if (ctx.StopCts.IsCancellationRequested)
        {
            state = target is not null ? StateCompleted : StateStopped;
            reason = target ?? "UserStop";
        }
        else
        {
            state = StateCompleted;
            reason = target ?? (p.RunUntilStopped ? "UserStop" : "TargetMessages");
        }

        var (cp50, cp95, cp99) = Percentiles.P50P95P99(commitMs);
        var (bp50, bp95, bp99) = Percentiles.P50P95P99(batchMs);
        var results = new RunResults
        {
            RunId = p.RunId,
            Profile = p,
            State = state,
            StopReason = reason,
            StartedAt = ctx.StartedAt,
            FinishedAt = finished,
            DurationSeconds = durationSec,
            DispatchedMessages = dispatched,
            WrittenMessages = written,
            AlreadyCommittedDuplicates = alreadyCommitted,
            AmbiguousUnresolved = ambiguous,
            FailedMessages = failed,
            BytesWritten = bytes,
            Batches = batches,
            Commits = commits,
            Reconnects = reconnects,
            CommitMeanMs = commitMs.Count > 0 ? commitMs.Average() : 0,
            CommitP50Ms = cp50,
            CommitP95Ms = cp95,
            CommitP99Ms = cp99,
            BatchMeanMs = batchMs.Count > 0 ? batchMs.Average() : 0,
            BatchP50Ms = bp50,
            BatchP95Ms = bp95,
            BatchP99Ms = bp99,
            ActualRatePerSecond = written / durationSec,
            ActualBytesPerSecond = bytes / durationSec,
            Errors = errors,
        };
        writtenIds.ForEach(results.WrittenIds.Add);
        ambiguousIds.ForEach(results.AmbiguousIds.Add);
        failedIds.ForEach(results.FailedIds.Add);

        lock (_gate)
        {
            _lastResults = results;
            _lastStopReason = reason;
            _current = null;
            _state = state;
        }
        metrics.GenerationActive.Set(0);

        logger.LogInformation(
            "Прогон {RunId} завершён: {State} ({Reason}): written={Written} ({Bytes} B) за {Dur:F1}s = {Rate:F1} msg/s; alreadyCommitted={AC} ambiguous={Amb} failed={Fail}; commit p50/p95/p99 = {P50:F1}/{P95:F1}/{P99:F1} ms; reconnects={Re}",
            p.RunId, state, reason, written, bytes, durationSec, results.ActualRatePerSecond,
            alreadyCommitted, ambiguous, failed, cp50, cp95, cp99, reconnects);
    }

    private static string Trunc(string s) => s.Length <= 300 ? s : s[..300];

    /// <summary>
    /// Мутабельное состояние одного прогона; после финализации больше не меняется.
    /// </summary>
    private sealed class RunContext
    {
        public required LoadProfile Profile { get; init; }
        public required CancellationTokenSource HardCts { get; init; }
        public required CancellationTokenSource StopCts { get; init; }
        public required SemaphoreSlim Sem { get; init; }
        public DateTimeOffset StartedAt { get; init; }
        public Task RunTask { get; set; } = Task.CompletedTask;

        public readonly object Sync = new();
        public volatile bool Fatal;
        public string? FatalError;

        public long Dispatched, DispatchedBytes, Written, Bytes, AlreadyCommitted, Ambiguous, Failed;
        public int Batches, Commits, Reconnects;

        public readonly List<long> WrittenIds = new();
        public readonly List<long> AmbiguousIds = new();
        public readonly List<long> FailedIds = new();
        public readonly List<double> CommitMs = new();
        public readonly List<double> BatchMs = new();
        public readonly List<string> ErrorMessages = new();
        public readonly List<Task> Tasks = new();
    }
}
