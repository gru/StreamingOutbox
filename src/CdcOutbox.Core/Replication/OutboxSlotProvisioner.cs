using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Processing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CdcOutbox.Core.Replication;

/// <summary>
/// Создаёт слот репликации при старте, если тот отсутствует.
/// </summary>
public sealed class OutboxSlotProvisioner(
    IOptions<PostgresOptions> postgres,
    IOptions<ReplicationOptions> replication,
    ILogger<OutboxSlotProvisioner> logger)
{
    private readonly PostgresOptions _postgres = postgres.Value;
    private readonly ReplicationOptions _replication = replication.Value;

    /// <summary>
    /// Создаёт слот при отсутствии и валидирует существующий.
    /// </summary>
    /// <param name="cancellationToken">Токен остановки.</param>
    public async Task EnsureAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_postgres.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        if (await IsInRecoveryAsync(connection, cancellationToken))
        {
            throw new InvalidOperationException(
                "Соединение с PostgreSQL указывает на сервер в recovery (pg_is_in_recovery=t), а не на primary: " +
                "логический слот создаётся и потребляется только на primary. " +
                "Проверь, что строка подключения указывает на primary.");
        }

        var existing = await ReadSlotAsync(connection, cancellationToken);
        if (existing is not null)
        {
            AcceptExistingSlot(existing);
            return;
        }

        try
        {
            var restartLsn = await CreateSlotAsync(connection, cancellationToken);
            logger.LogInformation(
                "Слот '{SlotName}' создан (plugin=pgoutput, failover=true, restart_lsn={RestartLsn}).",
                _replication.SlotName, restartLsn);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.DuplicateObject)
        {
            existing = await ReadSlotAsync(connection, cancellationToken);
            if (existing is null)
            {
                throw;
            }

            logger.LogInformation("Слот '{SlotName}' создан параллельным экземпляром.", _replication.SlotName);
            AcceptExistingSlot(existing);
        }
        catch (PostgresException ex)
        {
            var mapped = MapSlotCreationIssue(ex.SqlState, ex.MessageText);
            if (mapped is not null)
            {
                throw new StartupCheckException($"{mapped} Ответ сервера: {ex.MessageText}", ex);
            }

            throw;
        }
    }

    /// <summary>
    /// Возвращает запись слота из каталога pg_replication_slots.
    /// </summary>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Запись слота; null, если слот отсутствует.</returns>
    public async Task<SlotCatalogEntry?> FindSlotAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_postgres.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        return await ReadSlotAsync(connection, cancellationToken);
    }

    /// <summary>
    /// Возвращает confirmed_flush слота.
    /// </summary>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Позиция confirmed_flush; null, если слота нет или confirmed_flush пуст.</returns>
    public async Task<ulong?> GetConfirmedFlushAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_postgres.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            connection);
        command.Parameters.Add(new() { Value = _replication.SlotName });

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null || result is DBNull ? null : LsnText.TryParse(result.ToString());
    }

    /// <summary>
    /// Возвращает разницу confirmed_flush слота с переданной позицией, байт.
    /// </summary>
    /// <param name="lsn">Позиция для сравнения.</param>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Разница: положительная — позиция позади confirmed_flush, отрицательная — опережает; null, если сравнивать не с чем.</returns>
    public async Task<long?> GetConfirmedFlushDiffAsync(ulong lsn, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_postgres.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        // сравнение выполняет сервер: текстовую форму LSN разбирает парсер
        // pg_lsn самой реализации PostgreSQL — сервис не зависит от формата вывода
        await using var command = new NpgsqlCommand(
            "SELECT pg_wal_lsn_diff(confirmed_flush_lsn, $1::pg_lsn) FROM pg_replication_slots WHERE slot_name = $2",
            connection);
        command.Parameters.Add(new() { Value = LsnText.Format(lsn) });
        command.Parameters.Add(new() { Value = _replication.SlotName });

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null || result is DBNull ? null : Convert.ToInt64(result);
    }

    /// <summary>
    /// Возвращает описание проблемы существующего слота с плагином, отличным от pgoutput.
    /// </summary>
    /// <param name="slotName">Имя слота.</param>
    /// <param name="plugin">Плагин слота; null, если в каталоге пусто.</param>
    /// <returns>Описание проблемы; null, если слот пригоден.</returns>
    public static string? FindExistingSlotIssue(string slotName, string? plugin)
    {
        if (plugin != "pgoutput")
        {
            return $"Слот '{slotName}' существует с плагином '{plugin ?? "<пусто>"}', ожидается 'pgoutput'. " +
                   "Пересоздание не выполняется автоматически — устрани вручную и перезапусти сервис.";
        }

        return null;
    }

    /// <summary>
    /// Возвращает предупреждение о существующем слоте без failover.
    /// </summary>
    /// <param name="slotName">Имя слота.</param>
    /// <param name="failover">Флаг failover слота; null, если в каталоге пусто.</param>
    /// <returns>Текст предупреждения; null, если failover включён.</returns>
    public static string? FindSlotFailoverWarning(string slotName, bool? failover)
    {
        if (failover == true)
        {
            return null;
        }

        return $"Слот '{slotName}' существует без failover: чтению флаг не требуется, но слот не синхронизируется " +
               "на standby и не переживёт promotion — после смены primary сервис создаст слот заново с текущей " +
               "позиции WAL, недоставленное окно до последнего подтверждения потребует восстановления вручную.";
    }

    /// <summary>
    /// Возвращает понятное описание известной ошибки создания слота (wal_level &lt; logical).
    /// </summary>
    /// <param name="sqlState">SQLSTATE ошибки PostgreSQL.</param>
    /// <param name="serverMessage">Сообщение сервера.</param>
    /// <returns>Описание причины; null, если ошибка не распознана.</returns>
    public static string? MapSlotCreationIssue(string? sqlState, string serverMessage)
    {
        if (sqlState == PostgresErrorCodes.ObjectNotInPrerequisiteState &&
            serverMessage.Contains("wal_level", StringComparison.OrdinalIgnoreCase))
        {
            return "Сервер PostgreSQL не готов к логическому декодированию: wal_level < logical. " +
                   "Включи wal_level=logical на сервере и перезапусти сервис.";
        }

        return null;
    }

    private void AcceptExistingSlot(SlotCatalogEntry slot)
    {
        var issue = FindExistingSlotIssue(_replication.SlotName, slot.Plugin);
        if (issue is not null)
        {
            throw new StartupCheckException(issue);
        }

        logger.LogInformation(
            "Слот '{SlotName}' уже существует (plugin={Plugin}, failover={Failover}).",
            _replication.SlotName, slot.Plugin, slot.Failover);

        var warning = FindSlotFailoverWarning(_replication.SlotName, slot.Failover);
        if (warning is not null)
        {
            logger.LogWarning("{Warning}", warning);
        }
    }

    private static async Task<bool> IsInRecoveryAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT pg_is_in_recovery()", connection);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is true;
    }

    private async Task<SlotCatalogEntry?> ReadSlotAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "SELECT plugin, failover FROM pg_replication_slots WHERE slot_name = $1",
            connection);
        command.Parameters.Add(new() { Value = _replication.SlotName });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        var plugin = reader.IsDBNull(0) ? null : reader.GetString(0);
        var failover = reader.IsDBNull(1) ? null : (bool?)reader.GetBoolean(1);
        return new SlotCatalogEntry(plugin, failover);
    }

    private async Task<string> CreateSlotAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "SELECT lsn FROM pg_create_logical_replication_slot(slot_name => $1, plugin => 'pgoutput', failover => true)",
            connection);
        command.Parameters.Add(new() { Value = _replication.SlotName });

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result?.ToString() ?? string.Empty;
    }
}
