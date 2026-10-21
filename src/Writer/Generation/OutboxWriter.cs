using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using Writer.Configuration;

namespace Writer.Generation;

public enum BatchOutcome
{
    /// <summary>
    /// Commit подтверждён в этой попытке.
    /// </summary>
    Committed,
    /// <summary>
    /// Unique violation по PK на повторной попытке — прошлый commit прошёл.
    /// </summary>
    AlreadyCommitted,
    /// <summary>
    /// Остановка во время ожидания ретрая — исход commit не определён.
    /// </summary>
    Cancelled,
    /// <summary>
    /// Нетранзиентная ошибка SQL.
    /// </summary>
    Fatal,
}

/// <summary>
/// Результат выполнения батча (детали семантики — docs/writer.md).
/// </summary>
public sealed record BatchResult(
    BatchOutcome Outcome,
    int Attempts,
    double BatchDurationMs,
    double CommitDurationMs,
    string? FatalError = null,
    string? LastTransientError = null);

/// <summary>
/// Выполнение INSERT-батчей в outbox.messages от sys_writer. Единственный
/// NpgsqlDataSource — напрямую через HAProxy (пул 72); соединения ленивые.
/// </summary>
/// <param name="options">Конфигурация подключения и прикладной БД.</param>
public sealed class OutboxWriter(WriterOptions options) : IAsyncDisposable
{
    /// <summary>
    /// 6 параметров на строку; протокол допускает до 65535.
    /// </summary>
    public const int MaxBatchSize = 5000;

    private static readonly ConcurrentDictionary<int, string> SqlCache = new();

    private const int PoolSize = 72;

    /// <summary>
    /// Единственный DataSource: напрямую через HAProxy; для /ready.
    /// </summary>
    public NpgsqlDataSource Source { get; } =
        BuildSource(options.Connection.Host, options.Connection.Port, options.Connection.Database, PoolSize);

    private volatile string? _sequenceName;

    private static NpgsqlDataSource BuildSource(string host, int port, DatabaseOptions db, int maxPool)
    {
        var csb = new NpgsqlConnectionStringBuilder
        {
            Host = host,
            Port = port,
            Database = db.Name,
            Username = db.Username,
            Password = db.Password,
            MaxPoolSize = maxPool,
            MinPoolSize = 2,
            Timeout = 15,
            CommandTimeout = 60,
        };
        return NpgsqlDataSource.Create(csb);
    }

    /// <summary>
    /// Резервирует count значений id у identity-последовательности outbox.messages.
    /// Возвращает те же id на повторных вызовах не обязан; вставка выполняется позже
    /// теми же id (см. WriteBatchAsync). Отмена — OperationCanceledException.
    /// </summary>
    public async Task<long[]> ReserveIdsAsync(int count, CancellationToken hardCt, CancellationToken stopCt)
    {
        if (count <= 0) return Array.Empty<long>();
        var source = Source;
        var attempts = 0;

        while (true)
        {
            attempts++;
            try
            {
                await using var conn = await source.OpenConnectionAsync(hardCt);
                var seqName = await ResolveSequenceNameAsync(conn, hardCt);
                await using var cmd = new NpgsqlCommand("SELECT nextval($1::regclass) FROM generate_series(1, $2)", conn);
                cmd.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = seqName });
                cmd.Parameters.Add(new NpgsqlParameter<int> { NpgsqlDbType = NpgsqlDbType.Integer, TypedValue = count });
                var ids = new long[count];
                var i = 0;
                await using var reader = await cmd.ExecuteReaderAsync(hardCt);
                while (await reader.ReadAsync(hardCt))
                    ids[i++] = reader.GetInt64(0);
                return i == count ? ids : ids[..i];
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(stopCt, hardCt);
                await Task.Delay(BackoffDelay(attempts), waitCts.Token);
            }
            catch (Exception ex)
            {
                var detail = ex is PostgresException pg ? $"{pg.SqlState}: {pg.MessageText}" : ex.Message;
                throw new InvalidOperationException($"резервирование id не удалось: {detail}", ex);
            }
        }
    }

    private async Task<string> ResolveSequenceNameAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        var cached = _sequenceName;
        if (cached is not null) return cached;
        await using var cmd = new NpgsqlCommand("SELECT pg_get_serial_sequence('outbox.messages', 'id')", conn);
        if (await cmd.ExecuteScalarAsync(ct) is not string seqName)
            throw new InvalidOperationException("pg_get_serial_sequence('outbox.messages','id') вернул NULL — таблица/identity-колонка не найдены");
        _sequenceName = seqName;
        return seqName;
    }

    /// <summary>
    /// Записывает батч одной транзакцией: multi-row INSERT → DELETE тех же строк → commit.
    /// Транзиентные сбои ретраятся теми же id. hardCt прерывает попытки немедленно;
    /// stopCt — только ожидание между ретраями.
    /// </summary>
    public async Task<BatchResult> WriteBatchAsync(GeneratedMessage[] batch,
        CancellationToken hardCt, CancellationToken stopCt)
    {
        var source = Source;
        var sql = SqlCache.GetOrAdd(batch.Length, BuildInsertSql);
        var ids = new long[batch.Length];
        for (var i = 0; i < batch.Length; i++) ids[i] = batch[i].Id;
        var attempts = 0;
        string? lastTransient = null;
        var totalSw = System.Diagnostics.Stopwatch.StartNew();

        while (true)
        {
            attempts++;
            try
            {
                await using var conn = await source.OpenConnectionAsync(hardCt);
                await using var tx = await conn.BeginTransactionAsync(hardCt);

                await using (var insert = new NpgsqlCommand(sql, conn, tx))
                {
                    AddParameters(insert, batch);
                    await insert.ExecuteNonQueryAsync(hardCt);
                }

                await using (var delete = new NpgsqlCommand(DeleteSql, conn, tx))
                {
                    delete.Parameters.Add(new NpgsqlParameter<long[]>
                    {
                        NpgsqlDbType = NpgsqlDbType.Bigint | NpgsqlDbType.Array,
                        TypedValue = ids,
                    });
                    await delete.ExecuteNonQueryAsync(hardCt);
                }

                var commitSw = System.Diagnostics.Stopwatch.StartNew();
                await tx.CommitAsync(hardCt);
                return new BatchResult(BatchOutcome.Committed, attempts,
                    totalSw.Elapsed.TotalMilliseconds, commitSw.Elapsed.TotalMilliseconds);
            }
            catch (OperationCanceledException) when (hardCt.IsCancellationRequested)
            {
                return new BatchResult(BatchOutcome.Cancelled, attempts,
                    totalSw.Elapsed.TotalMilliseconds, 0, LastTransientError: lastTransient);
            }
            catch (PostgresException pe) when (pe.SqlState == PostgresErrorCodes.UniqueViolation && attempts > 1)
            {
                return new BatchResult(BatchOutcome.AlreadyCommitted, attempts,
                    totalSw.Elapsed.TotalMilliseconds, 0, LastTransientError: lastTransient);
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                lastTransient = ex.Message;
                using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(stopCt, hardCt);
                try
                {
                    await Task.Delay(BackoffDelay(attempts), waitCts.Token);
                }
                catch (OperationCanceledException)
                {
                    return new BatchResult(BatchOutcome.Cancelled, attempts,
                        totalSw.Elapsed.TotalMilliseconds, 0, LastTransientError: lastTransient);
                }
            }
            catch (Exception ex)
            {
                var detail = ex is PostgresException pg ? $"{pg.SqlState}: {pg.MessageText}" : ex.Message;
                return new BatchResult(BatchOutcome.Fatal, attempts,
                    totalSw.Elapsed.TotalMilliseconds, 0, FatalError: detail, LastTransientError: lastTransient);
            }
        }
    }

    private const string DeleteSql = "DELETE FROM outbox.messages WHERE id = ANY($1)";

    private static string BuildInsertSql(int rows)
    {
        var sb = new StringBuilder(128 + rows * 52)
            .Append("INSERT INTO outbox.messages (id, topic, key, body, headers, created_at) OVERRIDING SYSTEM VALUE VALUES ");
        for (var i = 0; i < rows; i++)
        {
            if (i > 0) sb.Append(',');
            var p = i * 6;
            sb.Append("($").Append(p + 1).Append(", $").Append(p + 2).Append(", $")
              .Append(p + 3).Append(", $").Append(p + 4).Append(", $")
              .Append(p + 5).Append(", $").Append(p + 6).Append(')');
        }
        return sb.ToString();
    }

    private static void AddParameters(NpgsqlCommand cmd, GeneratedMessage[] batch)
    {
        foreach (var m in batch)
        {
            cmd.Parameters.Add(new NpgsqlParameter<long> { NpgsqlDbType = NpgsqlDbType.Bigint, TypedValue = m.Id });
            cmd.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = m.Topic });
            cmd.Parameters.Add(new NpgsqlParameter<byte[]> { NpgsqlDbType = NpgsqlDbType.Bytea, TypedValue = m.Key });
            cmd.Parameters.Add(new NpgsqlParameter<byte[]> { NpgsqlDbType = NpgsqlDbType.Bytea, TypedValue = m.Body });
            cmd.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = m.Headers });
            cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { NpgsqlDbType = NpgsqlDbType.TimestampTz, TypedValue = m.CreatedAt });
        }
    }

    /// <summary>
    /// Транзиентные для стенда сбои; ошибки прав/схемы/ограничений сюда не входят (список — docs/writer.md).
    /// </summary>
    private static bool IsTransient(Exception ex) => ex switch
    {
        PostgresException pe => TransientSqlStates.Contains(pe.SqlState),
        NpgsqlException { InnerException: SocketException or IOException or TimeoutException } => true,
        NpgsqlException { InnerException: PostgresException pe2 } => TransientSqlStates.Contains(pe2.SqlState),
        _ => false,
    };

    private static readonly HashSet<string> TransientSqlStates = new()
    {
        "08000", "08003", "08006", "08001", "08004", "08007",
        "53300",
        "57P01", "57P02", "57P03",
        "40001", "40P01",
        "55P03",
        "54000",
    };

    private static TimeSpan BackoffDelay(int attempt)
    {
        var cap = 5000.0;
        var ms = Math.Min(250.0 * (1L << Math.Min(attempt - 1, 14)), cap);
        var jittered = ms * (0.7 + 0.6 * Random.Shared.NextDouble());
        return TimeSpan.FromMilliseconds(Math.Clamp(jittered, 50, cap));
    }

    public async ValueTask DisposeAsync()
    {
        await Source.DisposeAsync();
    }
}
