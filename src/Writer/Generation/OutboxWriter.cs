using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using Writer.Configuration;

namespace Writer.Generation;

public enum BatchOutcome
{
    /// <summary>Commit подтверждён в этой попытке.</summary>
    Committed,
    /// <summary>Unique violation по PK на повторной попытке — доказательство, что «неоднозначный» commit прошлой попытки прошёл.</summary>
    AlreadyCommitted,
    /// <summary>Остановка (stop/shutdown) во время ожидания ретрая — исход commit не определён (ambiguous).</summary>
    Cancelled,
    /// <summary>Нетранзиентная ошибка SQL — повтор бессмыслен.</summary>
    Fatal,
}

public sealed record BatchResult(
    BatchOutcome Outcome,
    int Attempts,
    double BatchDurationMs,
    double CommitDurationMs,
    string? FatalError = null,
    string? LastTransientError = null);

/// <summary>
/// Выполнение INSERT-батчей в outbox.messages от sys_writer.
/// Два источника Npgsql: через PgBouncer (transaction pool) и напрямую через HAProxy
/// на актуальный primary; активный выбирает режим прогона.
///
/// id — bigint identity, но Writer вставляет явные зарезервированные значения
/// (OVERRIDING SYSTEM VALUE): id должен попасть в body/key до вставки и быть
/// стабильным на ретраях (детект AlreadyCommitted по PK-конфликту).
/// </summary>
public sealed class OutboxWriter : IAsyncDisposable
{
    /// <summary>6 параметров на строку (id, topic, key, body, headers, created_at); протокол допускает до 65535 — берём запас.</summary>
    public const int MaxBatchSize = 5000;

    private static readonly ConcurrentDictionary<int, string> SqlCache = new();

    private readonly NpgsqlDataSource _pgbouncerSource;
    private readonly NpgsqlDataSource _directSource;

    private volatile string? _sequenceName;

    public OutboxWriter(WriterOptions options)
    {
        // Пул фиксированно широк (parallelism ≤ 64 + запас на замену битых соединений);
        // соединения открываются лениво — неактивный режим ничего не держит.
        const int pool = 72;
        _pgbouncerSource = BuildSource(options.Connection.Pgbouncer, options.Connection.Database, viaPgBouncer: true, pool);
        _directSource = BuildSource(options.Connection.Direct, options.Connection.Database, viaPgBouncer: false, pool);
    }

    public NpgsqlDataSource SourceFor(string mode) =>
        string.Equals(mode, "direct", StringComparison.OrdinalIgnoreCase) ? _directSource : _pgbouncerSource;

    private static NpgsqlDataSource BuildSource(EndpointOptions ep, DatabaseOptions db, bool viaPgBouncer, int maxPool)
    {
        var csb = new NpgsqlConnectionStringBuilder
        {
            Host = ep.Host,
            Port = ep.Port,
            Database = db.Name,
            Username = db.Username,
            Password = db.Password,
            MaxPoolSize = maxPool,
            MinPoolSize = 2,
            Timeout = 15,
            CommandTimeout = 60,
            // PgBouncer transaction pool: серверная сессия живёт только внутри транзакции,
            // сброс состояния соединения Npgsql после возврата в пул не нужен и вреден
            // (рекомендация Npgsql для внешних пулов). AutoPrepare выключен по умолчанию —
            // именованных prepared statements через transaction pool не возникает.
            NoResetOnClose = viaPgBouncer,
        };
        return NpgsqlDataSource.Create(csb);
    }

    /// <summary>
    /// Резервирует count значений id у identity-последовательности таблицы outbox.messages
    /// (SELECT nextval). Id нужны ДО сборки body: идентичность сообщения (= id строки) попадает
    /// в body и key. nextval нетранзакционен — «потерянные» при отмене значения дают лишь
    /// пропуски в последовательности, это безопасно. Транзиентные сбои ретраятся тем же
    /// backoff'ом, что и запись; фатальные — исключение (диспетчер завершит прогон).
    /// </summary>
    public async Task<long[]> ReserveIdsAsync(string mode, int count, CancellationToken hardCt, CancellationToken stopCt)
    {
        if (count <= 0) return Array.Empty<long>();
        var source = SourceFor(mode);
        var attempts = 0;

        while (true)
        {
            attempts++;
            try
            {
                await using var conn = await source.OpenConnectionAsync(hardCt);
                var seqName = await ResolveSequenceNameAsync(conn, hardCt);
                await using var cmd = new NpgsqlCommand("SELECT nextval($1::regclass) FROM generate_series(1, $2)", conn);
                // Позиционные параметры (без имён) — совместимо с PgBouncer transaction pool.
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
                throw; // stop/hard-отмена: вставки не было, диспетчер корректно завершится
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(stopCt, hardCt);
                await Task.Delay(BackoffDelay(attempts), waitCts.Token); // OCE уйдёт наружу
            }
            catch (Exception ex)
            {
                var detail = ex is PostgresException pg ? $"{pg.SqlState}: {pg.MessageText}" : ex.Message;
                throw new InvalidOperationException($"резервирование id не удалось: {detail}", ex);
            }
        }
    }

    /// <summary>Имя identity-последовательности (кэшируется после первого вызова; both источника смотрят в одну БД).</summary>
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
    /// Таблица остаётся ПУСТОЙ: строка живёт только внутри транзакции, сообщение доставляется
    /// через WAL — publication publish='insert' эмитит только Insert, DELETE не эмитится вовсе
    /// (dependencies.md §4). Транзиентные сбои (failover/switchover, обрывы соединения)
    /// ретраяются теми же id с exponential backoff + jitter. Unique violation на ретрае
    /// означает, что прошлая попытка закоммитилась — это разрешение неоднозначного commit:
    /// противоречие невозможно, дубликатов в outbox не возникает.
    /// hardCt прерывает попытки немедленно (остановка хоста); stopCt прерывает только
    /// ожидание между ретраями (мягкий stop) — начавшийся commit всегда доигрывается.
    /// </summary>
    public async Task<BatchResult> WriteBatchAsync(string mode, GeneratedMessage[] batch,
        CancellationToken hardCt, CancellationToken stopCt)
    {
        var source = SourceFor(mode);
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

                // удаляем те же строки в этой же транзакции: таблица не растёт,
                // DELETE по PK только что вставленных идентификаторов дёшев
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
        // Позиционные параметры (без имён) — совместимо с PgBouncer transaction pool.
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
    /// Транзиентные для стенда сбои: обрывы TCP/таймауты (failover, рестарт PgBouncer/HAProxy,
    /// kill сессий) и классы ошибок PG 08xxx/шатдауны/перегрузка/локи.
    /// Ошибки прав/схемы/ограничений сюда НЕ входят — они фатальны.
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
        "08000", "08003", "08006", "08001", "08004", "08007", // connection_exception
        "53300",                                               // too_many_connections
        "57P01", "57P02", "57P03",                             // admin/crash shutdown, cannot_connect_now (Patroni failover/switchover)
        "40001", "40P01",                                      // serialization_failure, deadlock_detected
        "55P03",                                               // lock_not_available
        "54000",                                               // out_of_memory / program limit
    };

    private static TimeSpan BackoffDelay(int attempt)
    {
        var cap = 5000.0;
        var ms = Math.Min(250.0 * (1L << Math.Min(attempt - 1, 14)), cap);
        var jittered = ms * (0.7 + 0.6 * Random.Shared.NextDouble()); // ±30%
        return TimeSpan.FromMilliseconds(Math.Clamp(jittered, 50, cap));
    }

    public async ValueTask DisposeAsync()
    {
        await _pgbouncerSource.DisposeAsync();
        await _directSource.DisposeAsync();
    }
}
