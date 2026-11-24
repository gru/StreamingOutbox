using System.Data;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace CdcOutbox.Core.Partitions;

/// <summary>
/// 24/7 автоматическое восстановление без потерь при пропаже, инвалидации (wal_status=lost)
/// или скачке позиции слота: блокирует уборщик партиций, пересоздаёт failover-слот,
/// вычитывает пропущенное окно из сохранённых часовых партиций в REPEATABLE READ снапшоте,
/// доставляет его в Kafka, обновляет checkpoint и снимает блокировку уборщика.
/// </summary>
public sealed class PartitionRecoveryService(
    IOptions<PostgresOptions> postgresOptions,
    IOptions<OutboxStorageOptions> storageOptions,
    IOptions<PartitionMaintenanceOptions> partitionOptions,
    OutboxSlotProvisioner slotProvisioner,
    IPartitionRegistryClient registryClient,
    OutboxPublisher publisher,
    CheckpointStore checkpointStore,
    ReplicationProgress progress,
    TimeProvider timeProvider,
    OutboxMetrics metrics,
    ILogger<PartitionRecoveryService> logger)
{
    private readonly string _connectionString = postgresOptions.Value.ConnectionString;
    private readonly string _schema = storageOptions.Value.Schema.Trim().ToLowerInvariant();
    private readonly string _table = storageOptions.Value.Table.Trim().ToLowerInvariant();
    private readonly PartitionMaintenanceOptions _options = partitionOptions.Value;

    /// <summary>
    /// Выполнить автоматическое восстановление слота и дочитку пропущенных сообщений из часовых партиций.
    /// </summary>
    /// <param name="previousCheckpointLsn">Последний известный подтверждённый LSN до разрыва (для логирования).</param>
    /// <param name="reason">Причина запуска авто-восстановления.</param>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Стартовый LSN нового failover-слота.</returns>
    public async Task<ulong> RecoverAsync(
        ulong? previousCheckpointLsn,
        string reason,
        CancellationToken cancellationToken)
    {
        if (!OutboxStorageOptionsValidator.IsSafeIdentifier(_schema) || !OutboxStorageOptionsValidator.IsSafeIdentifier(_table))
        {
            throw new InvalidOperationException($"Недопустимое имя таблицы outbox для восстановления: '{_schema}.{_table}'.");
        }

        logger.LogWarning(
            "Запуск 24/7 авто-восстановления из часовых партиций {Schema}.{Table} (причина: {Reason}; прошлый checkpoint: {OldLsn}).",
            _schema,
            _table,
            reason,
            previousCheckpointLsn is { } oldLsn ? LsnText.Format(oldLsn) : "<не было>");

        var systemIdentifier = await slotProvisioner.GetSystemIdentifierAsync(cancellationToken);
        var guard = await registryClient.ReadGuardStateAsync(cancellationToken);

        // 1. Блокируем уборщик партиций на всё время восстановления (защита от удаления невычитанных часов)
        await registryClient.SetRecoveryLockAsync(
            inProgress: true,
            systemIdentifier,
            resetContiguousLsn: null,
            resetLastCreatedAtUtc: null,
            cancellationToken);

        // 2. Гарантируем наличие партиций на 48ч вперёд, чтобы живые INSERT писателей не останавливались
        var ensure = await registryClient.EnsurePartitionsAsync(timeProvider.GetUtcNow(), cancellationToken);
        metrics.SetFuturePartitionsCount(ensure.FuturePartitionsCount);
        metrics.RegisterPartitionsCreated(ensure.CreatedCount);

        // 3. Сносим мёртвый/устаревший слот и создаём новый failover-слот (LSN_A)
        await slotProvisioner.DropSlotIfExistsAsync(cancellationToken);
        var newSlotLsn = await slotProvisioner.CreateFailoverSlotAndReturnLsnAsync(cancellationToken);

        // 4. Сразу открываем REPEATABLE READ снапшот (LSN_B >= LSN_A):
        // математически гарантирует видимость 100% транзакций, закоммиченных до LSN_A,
        // а всё после LSN_A гарантированно пойдёт в новый failover-слот
        var anchorUtc = progress.LastDeliveredCreatedAtUtc ?? guard?.LastDeliveredCreatedAtUtc;
        var cutoffUtc = anchorUtc is { } anchor
            ? anchor - TimeSpan.FromMinutes(_options.RecoveryOverlapMinutes)
            : (DateTime?)null;

        var (replayedCount, maxReplayedCreatedAtUtc) = await ReplayFromSnapshotAsync(cutoffUtc, cancellationToken);

        // 5. Атомарно фиксируем новый checkpoint в Kafka и обновляем прогресс
        await checkpointStore.SaveTransactionAsync(newSlotLsn, cancellationToken);
        var nowUtc = timeProvider.GetUtcNow().UtcDateTime;
        progress.MarkRead(newSlotLsn);
        progress.MarkDelivered(newSlotLsn);
        progress.MarkCheckpointed(newSlotLsn, nowUtc);
        metrics.RegisterCheckpointSaved();

        // 6. Только ПОСЛЕ полной доставки дыры и сохранения checkpoint снимаем блокировку с уборщика
        var finalCreatedAt = maxReplayedCreatedAtUtc ?? anchorUtc ?? nowUtc;
        await registryClient.SetRecoveryLockAsync(
            inProgress: false,
            systemIdentifier,
            resetContiguousLsn: newSlotLsn,
            resetLastCreatedAtUtc: finalCreatedAt,
            cancellationToken);

        metrics.RegisterPartitionAutoRecovery(replayedCount);
        logger.LogWarning(
            "24/7 авто-восстановление завершено: из партиций {Schema}.{Table} доставлено {Count} сообщений (cutoff={Cutoff}), " +
            "новый failover-слот и checkpoint установлены на LSN {NewLsn}, блокировка уборщика снята.",
            _schema,
            _table,
            replayedCount,
            cutoffUtc?.ToString("O") ?? "<все партиции>",
            LsnText.Format(newSlotLsn));

        return newSlotLsn;
    }

    private async Task<(long ReplayedCount, DateTime? MaxCreatedAtUtc)> ReplayFromSnapshotAsync(
        DateTime? cutoffUtc,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);

        var sql = cutoffUtc.HasValue
            ? $"SELECT id, topic, key, body, headers, partition, producer_key, created_at " +
              $"FROM {_schema}.{_table} WHERE created_at >= $1 ORDER BY created_at, id"
            : $"SELECT id, topic, key, body, headers, partition, producer_key, created_at " +
              $"FROM {_schema}.{_table} ORDER BY created_at, id";

        await using var command = new NpgsqlCommand(sql, connection, transaction)
        {
            CommandTimeout = 0,
        };
        if (cutoffUtc.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTimeOffset>
            {
                NpgsqlDbType = NpgsqlDbType.TimestampTz,
                TypedValue = new DateTimeOffset(DateTime.SpecifyKind(cutoffUtc.Value, DateTimeKind.Utc)),
            });
        }

        long totalReplayed = 0;
        DateTime? maxCreatedAtUtc = null;
        var batch = new List<OutboxMessage>(_options.RecoveryBatchSize);

        await using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetInt64(0);
                var topic = reader.GetString(1);
                var key = await ReadFieldBytesAsync(reader, 2, cancellationToken);
                var body = await ReadFieldBytesAsync(reader, 3, cancellationToken);
                var headers = reader.GetString(4);
                var partition = reader.IsDBNull(5) ? (int?)null : reader.GetInt32(5);
                var producerKey = reader.IsDBNull(6) ? null : reader.GetString(6);
                var createdAt = reader.GetDateTime(7).ToUniversalTime();

                if (maxCreatedAtUtc is null || createdAt > maxCreatedAtUtc.Value)
                {
                    maxCreatedAtUtc = createdAt;
                }

                batch.Add(new OutboxMessage
                {
                    Id = id,
                    Topic = topic,
                    Key = key,
                    Body = body,
                    Headers = headers,
                    Partition = partition,
                    ProducerKey = producerKey,
                    CreatedAt = createdAt,
                });

                if (batch.Count >= _options.RecoveryBatchSize)
                {
                    await publisher.PublishRecoveryBatchAsync(batch, cancellationToken);
                    totalReplayed += batch.Count;
                    batch.Clear();
                }
            }
        }

        if (batch.Count > 0)
        {
            await publisher.PublishRecoveryBatchAsync(batch, cancellationToken);
            totalReplayed += batch.Count;
            batch.Clear();
        }

        await transaction.CommitAsync(cancellationToken);
        return (totalReplayed, maxCreatedAtUtc);
    }

    private static async Task<byte[]> ReadFieldBytesAsync(
        NpgsqlDataReader reader,
        int ordinal,
        CancellationToken cancellationToken)
    {
        await using var stream = reader.GetStream(ordinal);
        using var memory = new MemoryStream();
        await stream.CopyToAsync(memory, cancellationToken);
        return memory.ToArray();
    }
}
