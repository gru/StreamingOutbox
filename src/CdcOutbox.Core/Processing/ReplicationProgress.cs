namespace CdcOutbox.Core.Processing;

/// <summary>
/// Состояние конвейера доставки.
/// </summary>
public sealed class ReplicationProgress
{
    private readonly SemaphoreSlim _deliverySignal = new(0);
    private readonly SemaphoreSlim _checkpointSignal = new(0);
    private long _readPosition;
    private long _deliveredPosition;
    private long _checkpointedPosition;
    private long _lastCheckpointUtcTicks;

    /// <summary>
    /// Прочитанная позиция WAL.
    /// </summary>
    public ulong ReadPosition => (ulong)Interlocked.Read(ref _readPosition);

    /// <summary>
    /// Позиция WAL, до которой все сообщения доставлены в Kafka.
    /// </summary>
    public ulong DeliveredPosition => (ulong)Interlocked.Read(ref _deliveredPosition);

    /// <summary>
    /// Позиция последнего подтверждённо сохранённого checkpoint.
    /// </summary>
    public ulong CheckpointedPosition => (ulong)Interlocked.Read(ref _checkpointedPosition);

    /// <summary>
    /// Момент последнего подтверждённо сохранённого checkpoint; null, если сохранений не было.
    /// </summary>
    public DateTime? LastCheckpointAtUtc =>
        Interlocked.Read(ref _lastCheckpointUtcTicks) is var ticks && ticks != 0
            ? new DateTime(ticks, DateTimeKind.Utc)
            : null;

    /// <summary>
    /// Зафиксировать прочитанную позицию WAL.
    /// </summary>
    /// <param name="lsn">Позиция WAL.</param>
    public void MarkRead(ulong lsn) => MoveForward(ref _readPosition, lsn);

    /// <summary>
    /// Зафиксировать позицию доставки.
    /// </summary>
    /// <param name="lsn">Позиция, до которой всё доставлено.</param>
    public void MarkDelivered(ulong lsn)
    {
        MoveForward(ref _deliveredPosition, lsn);
        _deliverySignal.Release();
    }

    /// <summary>
    /// Зафиксировать подтверждённо сохранённый checkpoint.
    /// </summary>
    /// <param name="lsn">Сохранённая позиция.</param>
    /// <param name="checkpointedAtUtc">Момент сохранения.</param>
    public void MarkCheckpointed(ulong lsn, DateTime checkpointedAtUtc)
    {
        MoveForward(ref _checkpointedPosition, lsn);
        MoveForward(ref _lastCheckpointUtcTicks, checkpointedAtUtc.ToUniversalTime().Ticks);
        _checkpointSignal.Release();
    }

    /// <summary>
    /// Дождаться продвижения водяного знака доставки (true) или таймаута (false).
    /// </summary>
    /// <param name="timeout">Ожидание.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>true, если сигнал получен; false по таймауту.</returns>
    public async Task<bool> WaitForDeliveryAsync(TimeSpan timeout, CancellationToken cancellationToken) =>
        await _deliverySignal.WaitAsync(timeout, cancellationToken);

    /// <summary>
    /// Дождаться нового сохранённого checkpoint (true) или таймаута (false).
    /// </summary>
    /// <param name="timeout">Ожидание.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>true, если сигнал получен; false по таймауту.</returns>
    public async Task<bool> WaitForCheckpointAsync(TimeSpan timeout, CancellationToken cancellationToken) =>
        await _checkpointSignal.WaitAsync(timeout, cancellationToken);

    private static void MoveForward(ref long target, ulong value)
    {
        var desired = unchecked((long)value);
        while (true)
        {
            var current = Interlocked.Read(ref target);
            if (current >= desired || Interlocked.CompareExchange(ref target, desired, current) == current)
            {
                return;
            }
        }
    }

    private static void MoveForward(ref long target, long value)
    {
        while (true)
        {
            var current = Interlocked.Read(ref target);
            if (current >= value || Interlocked.CompareExchange(ref target, value, current) == current)
            {
                return;
            }
        }
    }
}
