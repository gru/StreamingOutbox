using CdcOutbox.Core.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CdcOutbox.Core.Replication;

/// <summary>
/// Идемпотентное обеспечение слота логической репликации при старте сервиса:
/// отсутствующий слот создаётся (плагин pgoutput, failover=true), существующий
/// проверяется по плагину — расхождение явная ошибка, пересоздание чужого слота
/// не выполняется. Слот без failover принимается: чтению флаг не требуется,
/// выдаётся предупреждение (слот не переживёт promotion).
/// </summary>
public sealed class OutboxSlotProvisioner
{
    private readonly PostgresOptions _postgres;
    private readonly ReplicationOptions _replication;
    private readonly ILogger<OutboxSlotProvisioner> _logger;

    /// <summary>
    /// Создать provisioner слота.
    /// </summary>
    /// <param name="postgres">Подключение к PostgreSQL.</param>
    /// <param name="replication">Имена объектов репликации.</param>
    /// <param name="logger">Журнал.</param>
    public OutboxSlotProvisioner(
        IOptions<PostgresOptions> postgres,
        IOptions<ReplicationOptions> replication,
        ILogger<OutboxSlotProvisioner> logger)
    {
        _postgres = postgres.Value;
        _replication = replication.Value;
        _logger = logger;
    }

    /// <summary>
    /// Убедиться, что слот существует и пригоден для чтения сервисом.
    /// Требует соединения с primary в прикладной БД строки подключения
    /// (сервер в recovery — исключение).
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
                "логический слот создаётся и потребляется только на primary (в PG17 создание слота на standby " +
                "не завершается). Проверь, что строка подключения указывает на primary.");
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
            _logger.LogInformation(
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

            _logger.LogInformation("Слот '{SlotName}' создан параллельным экземпляром.", _replication.SlotName);
            AcceptExistingSlot(existing);
        }
        catch (PostgresException ex)
        {
            var mapped = MapSlotCreationIssue(ex.SqlState, ex.MessageText);
            if (mapped is not null)
            {
                throw new InvalidOperationException($"{mapped} Ответ сервера: {ex.MessageText}", ex);
            }

            throw;
        }
    }

    /// <summary>
    /// Проверить существующий слот по плагину каталога pg_replication_slots:
    /// декодер сервиса работает только с pgoutput.
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
    /// Предупреждение о существующем слоте без failover: чтению флаг не требуется,
    /// но слот не синхронизируется на standby и не переживёт promotion.
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
    /// Перевести ошибку создания слота в понятное описание, если она известна
    /// (сервер без wal_level=logical).
    /// </summary>
    /// <param name="sqlState">SQLSTATE ошибки PostgreSQL.</param>
    /// <param name="serverMessage">Сообщение сервера.</param>
    /// <returns>Понятное описание причины; null, если ошибка не распознана.</returns>
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

    private void AcceptExistingSlot(SlotRow slot)
    {
        var issue = FindExistingSlotIssue(_replication.SlotName, slot.Plugin);
        if (issue is not null)
        {
            throw new InvalidOperationException(issue);
        }

        _logger.LogInformation(
            "Слот '{SlotName}' уже существует (plugin={Plugin}, failover={Failover}).",
            _replication.SlotName, slot.Plugin, slot.Failover);

        var warning = FindSlotFailoverWarning(_replication.SlotName, slot.Failover);
        if (warning is not null)
        {
            _logger.LogWarning("{Warning}", warning);
        }
    }

    private static async Task<bool> IsInRecoveryAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT pg_is_in_recovery()", connection);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is true;
    }

    private async Task<SlotRow?> ReadSlotAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
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
        return new SlotRow(plugin, failover);
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

    private sealed record SlotRow(string? Plugin, bool? Failover);
}
