using System.Globalization;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace CdcOutbox.Core.Partitions;

/// <summary>
/// Реализация операций управления почасовыми партициями и защитным курсором в PostgreSQL
/// (поддерживает как SECURITY DEFINER-функции БД, так и прямой DDL от имени владельца).
/// </summary>
public sealed class PartitionRegistryClient(
    IOptions<PostgresOptions> postgresOptions,
    IOptions<ReplicationOptions> replicationOptions,
    IOptions<OutboxStorageOptions> storageOptions,
    IOptions<PartitionMaintenanceOptions> partitionOptions,
    ILogger<PartitionRegistryClient> logger) : IPartitionRegistryClient
{
    private readonly string _connectionString = postgresOptions.Value.ConnectionString;
    private readonly string _slotName = replicationOptions.Value.SlotName;
    private readonly string _schema = RequireSafeIdentifier(storageOptions.Value.Schema, "CdcOutbox:Storage:Schema");
    private readonly string _table = RequireSafeIdentifier(storageOptions.Value.Table, "CdcOutbox:Storage:Table");
    private readonly string _registryTable = RequireSafeIdentifier(partitionOptions.Value.RegistryTable, "CdcOutbox:Partitions:RegistryTable");
    private readonly string _guardTable = RequireSafeIdentifier(partitionOptions.Value.GuardTable, "CdcOutbox:Partitions:GuardTable");
    private readonly PartitionMaintenanceOptions _options = partitionOptions.Value;

    /// <inheritdoc />
    public async Task<long> GetSystemIdentifierAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand("SELECT system_identifier FROM pg_control_system()", connection);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    /// <inheritdoc />
    public async Task<PartitionEnsureResult> EnsurePartitionsAsync(DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);

        if (_options.UseSecurityDefinerFunctions)
        {
            await using var command = new NpgsqlCommand(
                $"SELECT created_count, future_count FROM {_schema}.ensure_partitions($1, $2, $3, $4)",
                connection);
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _schema });
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _table });
            command.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { NpgsqlDbType = NpgsqlDbType.TimestampTz, TypedValue = nowUtc.ToUniversalTime() });
            command.Parameters.Add(new NpgsqlParameter<int> { NpgsqlDbType = NpgsqlDbType.Integer, TypedValue = _options.PrecreateHours });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return new PartitionEnsureResult(0, 0);
            }

            return new PartitionEnsureResult(reader.GetInt32(0), reader.GetInt32(1));
        }

        await EnsureDirectMetadataTablesAsync(connection, cancellationToken);

        var windows = HourlyPartitionName.BuildHorizon(_table, nowUtc, _options.PrecreateHours);
        var createdCount = 0;

        foreach (var window in windows)
        {
            var partName = RequireSafeIdentifier(window.PartitionName, "partitionName");
            var existed = await PartitionTableExistsAsync(connection, partName, cancellationToken);
            if (!existed)
            {
                var startLiteral = FormatUtcTimestampLiteral(window.RangeStartUtc);
                var endLiteral = FormatUtcTimestampLiteral(window.RangeEndUtc);
                var ddl =
                    $"CREATE TABLE IF NOT EXISTS {_schema}.{partName} " +
                    $"PARTITION OF {_schema}.{_table} " +
                    $"FOR VALUES FROM ('{startLiteral}') TO ('{endLiteral}')";
                await using var createCommand = new NpgsqlCommand(ddl, connection);
                await createCommand.ExecuteNonQueryAsync(cancellationToken);
                createdCount++;
            }

            await using var registerCommand = new NpgsqlCommand(
                $"INSERT INTO {_schema}.{_registryTable} " +
                "(schema_name, parent_table, partition_name, range_start_utc, range_end_utc) " +
                "VALUES ($1, $2, $3, $4, $5) " +
                "ON CONFLICT (schema_name, parent_table, partition_name) DO NOTHING",
                connection);
            registerCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _schema });
            registerCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _table });
            registerCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = partName });
            registerCommand.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { NpgsqlDbType = NpgsqlDbType.TimestampTz, TypedValue = window.RangeStartUtc });
            registerCommand.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { NpgsqlDbType = NpgsqlDbType.TimestampTz, TypedValue = window.RangeEndUtc });
            await registerCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var currentHour = HourlyPartitionName.TruncateToHourUtc(nowUtc);
        await using var countCommand = new NpgsqlCommand(
            $"SELECT count(*)::integer FROM {_schema}.{_registryTable} " +
            "WHERE schema_name = $1 AND parent_table = $2 AND dropped_at_utc IS NULL AND range_start_utc > $3",
            connection);
        countCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _schema });
        countCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _table });
        countCommand.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { NpgsqlDbType = NpgsqlDbType.TimestampTz, TypedValue = currentHour });
        var futureCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);

        return new PartitionEnsureResult(createdCount, futureCount);
    }

    /// <inheritdoc />
    public async Task<int> SealClosedPartitionsAsync(DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        var cutoffUtc = nowUtc.ToUniversalTime().AddMinutes(-_options.SealGraceMinutes);
        await using var connection = await OpenConnectionAsync(cancellationToken);

        if (_options.UseSecurityDefinerFunctions)
        {
            await using var command = new NpgsqlCommand(
                $"SELECT {_schema}.seal_closed_partitions($1, $2, $3)",
                connection);
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _schema });
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _table });
            command.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { NpgsqlDbType = NpgsqlDbType.TimestampTz, TypedValue = cutoffUtc });
            return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        }

        await EnsureDirectMetadataTablesAsync(connection, cancellationToken);
        await using var directCommand = new NpgsqlCommand(
            $"UPDATE {_schema}.{_registryTable} " +
            "SET sealed_lsn = pg_current_wal_lsn(), sealed_at_utc = now() " +
            "WHERE schema_name = $1 AND parent_table = $2 AND dropped_at_utc IS NULL " +
            "AND sealed_lsn IS NULL AND range_end_utc <= $3",
            connection);
        directCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _schema });
        directCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _table });
        directCommand.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { NpgsqlDbType = NpgsqlDbType.TimestampTz, TypedValue = cutoffUtc });
        return await directCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<PartitionGuardState?> ReadGuardStateAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);

        NpgsqlCommand command;
        if (_options.UseSecurityDefinerFunctions)
        {
            command = new NpgsqlCommand(
                $"SELECT system_identifier, contiguous_delivered_lsn::text, last_delivered_created_at_utc, recovery_in_progress FROM {_schema}.read_partition_guard($1)",
                connection);
        }
        else
        {
            await EnsureDirectMetadataTablesAsync(connection, cancellationToken);
            command = new NpgsqlCommand(
                $"SELECT system_identifier, contiguous_delivered_lsn::text, last_delivered_created_at_utc, recovery_in_progress FROM {_schema}.{_guardTable} WHERE slot_name = $1",
                connection);
        }

        await using (command)
        {
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _slotName });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            var systemId = reader.GetInt64(0);
            var lsn = reader.IsDBNull(1) ? null : LsnText.TryParse(reader.GetString(1));
            var lastCreated = reader.IsDBNull(2) ? (DateTime?)null : reader.GetDateTime(2).ToUniversalTime();
            var recoveryInProgress = reader.GetBoolean(3);
            return new PartitionGuardState(systemId, lsn, lastCreated, recoveryInProgress);
        }
    }

    /// <inheritdoc />
    public async Task EnsureGuardInitializedAsync(
        long systemIdentifier,
        ulong? initialLsn,
        DateTime? initialCreatedAtUtc,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var lsnText = initialLsn is { } lsn && lsn > 0 ? LsnText.Format(lsn) : null;

        if (_options.UseSecurityDefinerFunctions)
        {
            await using var command = new NpgsqlCommand(
                $"SELECT {_schema}.ensure_partition_guard($1, $2, $3::pg_lsn, $4)",
                connection);
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _slotName });
            command.Parameters.Add(new NpgsqlParameter<long> { NpgsqlDbType = NpgsqlDbType.Bigint, TypedValue = systemIdentifier });
            command.Parameters.Add(CreateNullableTextParameter(lsnText));
            command.Parameters.Add(CreateNullableTimestampParameter(initialCreatedAtUtc));
            await command.ExecuteNonQueryAsync(cancellationToken);
            return;
        }

        await EnsureDirectMetadataTablesAsync(connection, cancellationToken);
        await using var directCommand = new NpgsqlCommand(
            $"INSERT INTO {_schema}.{_guardTable} " +
            "(slot_name, system_identifier, contiguous_delivered_lsn, last_delivered_created_at_utc, recovery_in_progress, updated_at_utc) " +
            "VALUES ($1, $2, $3::pg_lsn, $4, false, now()) " +
            "ON CONFLICT (slot_name) DO NOTHING",
            connection);
        directCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _slotName });
        directCommand.Parameters.Add(new NpgsqlParameter<long> { NpgsqlDbType = NpgsqlDbType.Bigint, TypedValue = systemIdentifier });
        directCommand.Parameters.Add(CreateNullableTextParameter(lsnText));
        directCommand.Parameters.Add(CreateNullableTimestampParameter(initialCreatedAtUtc));
        await directCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task UpdateDeliveryProgressAsync(
        ulong contiguousDeliveredLsn,
        DateTime? lastDeliveredCreatedAtUtc,
        long systemIdentifier,
        CancellationToken cancellationToken)
    {
        if (contiguousDeliveredLsn == 0)
        {
            return;
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        var lsnText = LsnText.Format(contiguousDeliveredLsn);

        if (_options.UseSecurityDefinerFunctions)
        {
            await using var command = new NpgsqlCommand(
                $"SELECT {_schema}.update_partition_guard_progress($1, $2, $3::pg_lsn, $4)",
                connection);
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _slotName });
            command.Parameters.Add(new NpgsqlParameter<long> { NpgsqlDbType = NpgsqlDbType.Bigint, TypedValue = systemIdentifier });
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = lsnText });
            command.Parameters.Add(CreateNullableTimestampParameter(lastDeliveredCreatedAtUtc));
            await command.ExecuteNonQueryAsync(cancellationToken);
            return;
        }

        await EnsureDirectMetadataTablesAsync(connection, cancellationToken);
        await using var directCommand = new NpgsqlCommand(
            $"UPDATE {_schema}.{_guardTable} " +
            "SET contiguous_delivered_lsn = CASE " +
            "        WHEN contiguous_delivered_lsn IS NULL OR $3::pg_lsn > contiguous_delivered_lsn THEN $3::pg_lsn " +
            "        ELSE contiguous_delivered_lsn END, " +
            "    last_delivered_created_at_utc = CASE " +
            "        WHEN $4 IS NULL THEN last_delivered_created_at_utc " +
            "        WHEN last_delivered_created_at_utc IS NULL OR $4 > last_delivered_created_at_utc THEN $4 " +
            "        ELSE last_delivered_created_at_utc END, " +
            "    updated_at_utc = now() " +
            "WHERE slot_name = $1 AND system_identifier = $2 AND recovery_in_progress = false",
            connection);
        directCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _slotName });
        directCommand.Parameters.Add(new NpgsqlParameter<long> { NpgsqlDbType = NpgsqlDbType.Bigint, TypedValue = systemIdentifier });
        directCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = lsnText });
        directCommand.Parameters.Add(CreateNullableTimestampParameter(lastDeliveredCreatedAtUtc));
        await directCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task SetRecoveryLockAsync(
        bool inProgress,
        long systemIdentifier,
        ulong? resetContiguousLsn,
        DateTime? resetLastCreatedAtUtc,
        CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var lsnText = resetContiguousLsn is { } lsn && lsn > 0 ? LsnText.Format(lsn) : null;

        if (_options.UseSecurityDefinerFunctions)
        {
            await using var command = new NpgsqlCommand(
                $"SELECT {_schema}.set_partition_recovery_lock($1, $2, $3, $4::pg_lsn, $5)",
                connection);
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _slotName });
            command.Parameters.Add(new NpgsqlParameter<bool> { NpgsqlDbType = NpgsqlDbType.Boolean, TypedValue = inProgress });
            command.Parameters.Add(new NpgsqlParameter<long> { NpgsqlDbType = NpgsqlDbType.Bigint, TypedValue = systemIdentifier });
            command.Parameters.Add(CreateNullableTextParameter(lsnText));
            command.Parameters.Add(CreateNullableTimestampParameter(resetLastCreatedAtUtc));
            await command.ExecuteNonQueryAsync(cancellationToken);
            return;
        }

        await EnsureDirectMetadataTablesAsync(connection, cancellationToken);
        await using var directCommand = new NpgsqlCommand(
            $"INSERT INTO {_schema}.{_guardTable} " +
            "(slot_name, system_identifier, contiguous_delivered_lsn, last_delivered_created_at_utc, recovery_in_progress, updated_at_utc) " +
            "VALUES ($1, $3, $4::pg_lsn, $5, $2, now()) " +
            "ON CONFLICT (slot_name) DO UPDATE SET " +
            "    recovery_in_progress = EXCLUDED.recovery_in_progress, " +
            $"    system_identifier = CASE WHEN EXCLUDED.recovery_in_progress = false THEN EXCLUDED.system_identifier ELSE {_schema}.{_guardTable}.system_identifier END, " +
            $"    contiguous_delivered_lsn = CASE WHEN EXCLUDED.recovery_in_progress = false AND EXCLUDED.contiguous_delivered_lsn IS NOT NULL THEN EXCLUDED.contiguous_delivered_lsn ELSE {_schema}.{_guardTable}.contiguous_delivered_lsn END, " +
            $"    last_delivered_created_at_utc = CASE WHEN EXCLUDED.last_delivered_created_at_utc IS NOT NULL THEN EXCLUDED.last_delivered_created_at_utc ELSE {_schema}.{_guardTable}.last_delivered_created_at_utc END, " +
            "    updated_at_utc = now()",
            connection);
        directCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _slotName });
        directCommand.Parameters.Add(new NpgsqlParameter<bool> { NpgsqlDbType = NpgsqlDbType.Boolean, TypedValue = inProgress });
        directCommand.Parameters.Add(new NpgsqlParameter<long> { NpgsqlDbType = NpgsqlDbType.Bigint, TypedValue = systemIdentifier });
        directCommand.Parameters.Add(CreateNullableTextParameter(lsnText));
        directCommand.Parameters.Add(CreateNullableTimestampParameter(resetLastCreatedAtUtc));
        await directCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> DropDeliveredPartitionsAsync(
        ulong safeContiguousLsn,
        long systemIdentifier,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken)
    {
        var guard = await ReadGuardStateAsync(cancellationToken);
        if (guard is null)
        {
            logger.LogDebug("Удаление партиций пропущено: защитный курсор слота '{SlotName}' ещё не инициализирован.", _slotName);
            return Array.Empty<string>();
        }

        if (guard.RecoveryInProgress)
        {
            logger.LogWarning(
                "Удаление партиций заблокировано (recovery_in_progress=true) для слота '{SlotName}' до завершения вычитки дыры.",
                _slotName);
            return Array.Empty<string>();
        }

        if (guard.SystemIdentifier != systemIdentifier)
        {
            logger.LogWarning(
                "Удаление партиций заблокировано: изменился system_identifier кластера ({SavedId} → {CurrentId}).",
                guard.SystemIdentifier,
                systemIdentifier);
            return Array.Empty<string>();
        }

        if (guard.ContiguousDeliveredLsn is not { } guardLsn || guardLsn == 0)
        {
            return Array.Empty<string>();
        }

        var effectiveSafeLsn = safeContiguousLsn > 0 ? Math.Min(safeContiguousLsn, guardLsn) : guardLsn;
        var dropBeforeUtc = nowUtc.ToUniversalTime().AddHours(-_options.RetentionHours);
        var lsnText = LsnText.Format(effectiveSafeLsn);

        await using var connection = await OpenConnectionAsync(cancellationToken);

        if (_options.UseSecurityDefinerFunctions)
        {
            var dropped = new List<string>();
            await using var command = new NpgsqlCommand(
                $"SELECT dropped_partition FROM {_schema}.drop_delivered_partitions($1, $2, $3, $4::pg_lsn, $5, $6)",
                connection);
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _schema });
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _table });
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _slotName });
            command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = lsnText });
            command.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { NpgsqlDbType = NpgsqlDbType.TimestampTz, TypedValue = dropBeforeUtc });
            command.Parameters.Add(new NpgsqlParameter<long> { NpgsqlDbType = NpgsqlDbType.Bigint, TypedValue = systemIdentifier });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                dropped.Add(reader.GetString(0));
            }

            return dropped;
        }

        return await DropDeliveredPartitionsDirectAsync(connection, lsnText, dropBeforeUtc, cancellationToken);
    }

    private async Task<IReadOnlyList<string>> DropDeliveredPartitionsDirectAsync(
        NpgsqlConnection connection,
        string safeLsnText,
        DateTimeOffset dropBeforeUtc,
        CancellationToken cancellationToken)
    {
        var candidates = new List<string>();
        await using (var selectCommand = new NpgsqlCommand(
            $"SELECT partition_name FROM {_schema}.{_registryTable} " +
            "WHERE schema_name = $1 AND parent_table = $2 AND dropped_at_utc IS NULL " +
            "AND sealed_lsn IS NOT NULL AND sealed_lsn <= $3::pg_lsn AND range_end_utc <= $4 " +
            "ORDER BY range_start_utc",
            connection))
        {
            selectCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _schema });
            selectCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _table });
            selectCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = safeLsnText });
            selectCommand.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { NpgsqlDbType = NpgsqlDbType.TimestampTz, TypedValue = dropBeforeUtc });

            await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                candidates.Add(reader.GetString(0));
            }
        }

        var dropped = new List<string>();
        foreach (var candidate in candidates)
        {
            var partName = RequireSafeIdentifier(candidate, "partition_name");
            try
            {
                // DETACH PARTITION CONCURRENTLY обязан выполняться в autocommit вне транзакционного блока
                await using (var detachCommand = new NpgsqlCommand(
                    $"ALTER TABLE {_schema}.{_table} DETACH PARTITION {_schema}.{partName} CONCURRENTLY",
                    connection))
                {
                    await detachCommand.ExecuteNonQueryAsync(cancellationToken);
                }
            }
            catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.UndefinedTable or PostgresErrorCodes.WrongObjectType or PostgresErrorCodes.InvalidParameterValue)
            {
                // партиция уже отцеплена или удалена ранее — переходим к DROP TABLE IF EXISTS
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)
            {
                logger.LogWarning(
                    "Не удалось отцепить партицию {Schema}.{Partition} из-за конкурентной блокировки — повторим на следующем запуске.",
                    _schema,
                    partName);
                continue;
            }

            await using (var dropCommand = new NpgsqlCommand(
                $"DROP TABLE IF EXISTS {_schema}.{partName}",
                connection))
            {
                await dropCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var markCommand = new NpgsqlCommand(
                $"UPDATE {_schema}.{_registryTable} SET dropped_at_utc = now() " +
                "WHERE schema_name = $1 AND parent_table = $2 AND partition_name = $3",
                connection))
            {
                markCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _schema });
                markCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _table });
                markCommand.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = partName });
                await markCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            dropped.Add(partName);
        }

        return dropped;
    }

    private async Task EnsureDirectMetadataTablesAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var registryCmd = new NpgsqlCommand(
            $"CREATE TABLE IF NOT EXISTS {_schema}.{_registryTable} (" +
            "schema_name text NOT NULL, " +
            "parent_table text NOT NULL, " +
            "partition_name text NOT NULL, " +
            "range_start_utc timestamptz NOT NULL, " +
            "range_end_utc timestamptz NOT NULL, " +
            "sealed_lsn pg_lsn, " +
            "sealed_at_utc timestamptz, " +
            "dropped_at_utc timestamptz, " +
            "PRIMARY KEY (schema_name, parent_table, partition_name))",
            connection);
        await registryCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var guardCmd = new NpgsqlCommand(
            $"CREATE TABLE IF NOT EXISTS {_schema}.{_guardTable} (" +
            "slot_name text PRIMARY KEY, " +
            "system_identifier bigint NOT NULL, " +
            "contiguous_delivered_lsn pg_lsn, " +
            "last_delivered_created_at_utc timestamptz, " +
            "recovery_in_progress boolean NOT NULL DEFAULT false, " +
            "updated_at_utc timestamptz NOT NULL DEFAULT now())",
            connection);
        await guardCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<bool> PartitionTableExistsAsync(
        NpgsqlConnection connection,
        string partitionName,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE n.nspname = $1 AND c.relname = $2)",
            connection);
        command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = _schema });
        command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = partitionName });
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static string RequireSafeIdentifier(string identifier, string paramName)
    {
        if (!OutboxStorageOptionsValidator.IsSafeIdentifier(identifier))
        {
            throw new ArgumentException($"Недопустимый SQL-идентификатор в {paramName}: '{identifier}'.", paramName);
        }

        return identifier.Trim().ToLowerInvariant();
    }

    private static string FormatUtcTimestampLiteral(DateTimeOffset utc) =>
        utc.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss+00", CultureInfo.InvariantCulture);

    private static NpgsqlParameter CreateNullableTextParameter(string? value) =>
        new()
        {
            NpgsqlDbType = NpgsqlDbType.Text,
            Value = value is null ? DBNull.Value : value,
        };

    private static NpgsqlParameter CreateNullableTimestampParameter(DateTime? value) =>
        new()
        {
            NpgsqlDbType = NpgsqlDbType.TimestampTz,
            Value = value is null
                ? DBNull.Value
                : new DateTimeOffset(DateTime.SpecifyKind(value.Value, DateTimeKind.Utc)),
        };
}
