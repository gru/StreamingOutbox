using System.Threading.Channels;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace CdcOutbox.Core.Replication;

/// <summary>
/// Читает слот логической репликации и записывает сообщения outbox в канал.
/// </summary>
public sealed class OutboxSlotReader(
    IOptions<PostgresOptions> postgresOptions,
    IOptions<ReplicationOptions> replicationOptions,
    IOptions<OutboxStorageOptions> storageOptions,
    IOptions<ProcessorOptions> processorOptions,
    ReplicationProgress progress,
    TimeProvider timeProvider,
    OutboxMetrics metrics,
    ILogger<OutboxSlotReader> logger) : IOutboxSlotReader
{
    private readonly string _connectionString = postgresOptions.Value.ConnectionString;
    private readonly string _slotName = replicationOptions.Value.SlotName;
    private readonly string _publicationName = replicationOptions.Value.PublicationName;
    private readonly bool _binary = replicationOptions.Value.Binary;
    private readonly string _schema = NormalizeIdentifier(storageOptions.Value.Schema);
    private readonly string _table = NormalizeIdentifier(storageOptions.Value.Table);
    private readonly TimeSpan _flushInterval = TimeSpan.FromSeconds(processorOptions.Value.CheckpointIntervalSeconds);
    private readonly TimeSpan _finalCheckpointWait = TimeSpan.FromSeconds(processorOptions.Value.FinalCheckpointWaitSeconds);
    private readonly TimeSpan _idleTimeout = TimeSpan.FromSeconds(processorOptions.Value.IdleTimeoutSeconds);
    private readonly IdleActivityTracker _idleTracker = new();

    /// <inheritdoc />
    public async Task ReadAsync(ChannelWriter<OutboxMessage> output, CancellationToken cancellationToken)
    {
        await using var connection = new LogicalReplicationConnection(_connectionString);
        await connection.Open(cancellationToken);

        var slot = new PgOutputReplicationSlot(_slotName);
        var options = new PgOutputReplicationOptions(_publicationName, PgOutputProtocolVersion.V1, binary: _binary ? true : null);

        // отдельный CancellationToken для graceful остановки репликации с flush чекпоинтов в kafka
        using var streamCts = new CancellationTokenSource();
        var stream = connection.StartReplication(slot, options, streamCts.Token);

        // подтверждение позиции слота — параллельный цикл на этом же соединении
        using var flushCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var flushing = FlushCheckpointAsync(connection, flushCts.Token);

        // idle-предупреждение — параллельный цикл, живёт со стримом и только логирует
        var idling = IdleWarnLoopAsync(flushCts.Token);

        var plans = new Dictionary<uint, OutboxColumnPlan>();
        var warnedRelations = new HashSet<string>(StringComparer.Ordinal);
        var transactionMessages = new List<OutboxMessage>();
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stopRegistration = cancellationToken.Register(
            static state => ((TaskCompletionSource)state!).TrySetResult(),
            stopped);

        await using var enumerator = stream.GetAsyncEnumerator(streamCts.Token);
        try
        {
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    // стоп до запроса следующего сообщения: операция репликации активна
                    await StopGracefullyAsync(output, connection);
                    break;
                }

                var moving = enumerator.MoveNextAsync().AsTask();
                var completed = await Task.WhenAny(moving, stopped.Task);
                if (completed != moving)
                {
                    // graceful shutdown: сначала завершающий handshake
                    // при живой операции, затем разрыв стрима
                    await StopGracefullyAsync(output, connection);
                    await streamCts.CancelAsync();
                    // разрыв стрима после завершающего handshake: операцию останавливаем мы,
                    // ошибка отмены ожидаема и наружу не пробрасывается
                    await SwallowAsync(moving);
                    break;
                }

                if (!await moving)
                {
                    break;
                }

                var message = enumerator.Current;
                // прочитанная позиция WAL — от неё считается отставание checkpoint
                progress.MarkRead((ulong)message.WalEnd);
                switch (message)
                {
                    case InsertMessage insert:
                        await TryReadInsertAsync(insert, plans, warnedRelations, transactionMessages, cancellationToken);
                        break;
                    case BeginMessage:
                        // новая транзакция: её сообщения буферизуются до Commit
                        transactionMessages.Clear();
                        break;
                    case CommitMessage commit:
                        await HandleCommitAsync(commit, transactionMessages, output, cancellationToken);
                        break;
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // отмена внутри обработки сообщения: итератор приостановлен на yield,
            // операция репликации ещё активна — тот же завершающий handshake
            await StopGracefullyAsync(output, connection);
        }
        finally
        {
            await flushCts.CancelAsync();
            // сбой фоновых циклов (подтверждение позиции, idle-предупреждение) не должен
            // маскировать результат чтения, а позицию дошлёт следующая сессия
            await SwallowAsync(flushing);
            await SwallowAsync(idling);
        }
    }

    internal static string NormalizeIdentifier(string identifier) => identifier.Trim().ToLowerInvariant();

    internal static bool IsOutboxRelation(string? @namespace, string? relationName, string schema, string table) =>
        string.Equals(@namespace, schema, StringComparison.Ordinal)
        && string.Equals(relationName, table, StringComparison.Ordinal);

    internal static string FindForeignRelationWarning(string @namespace, string relationName, string publicationName) =>
        $"В поток слота попала вставка чужой таблицы {@namespace}.{relationName} — проверь состав публикации '{publicationName}': кроме таблицы outbox (publish='insert') она ничего не должна публиковать.";

    internal static ReadingIssue FindReadingIssue(Exception exception) => exception switch
    {
        PostgresException { SqlState: PostgresErrorCodes.UndefinedObject } =>
            new(false, "Слот отсутствует или удалён (SQLSTATE 42704) — автопересоздание запрещено: пересоздание с текущей позиции потеряет непрочитанный WAL. Продолжаю попытки: слот может появиться после смены primary или вмешательства оператора."),
        PostgresException { SqlState: "55006" } postgresException =>
            new(false, $"Слот занят другим консюмером (SQLSTATE 55006: {postgresException.MessageText}) — повторное подключение после паузы."),
        PostgresException { SqlState: PostgresErrorCodes.ReadOnlySqlTransaction } =>
            new(false, "Подключение попало на read-only узел (SQLSTATE 25006) — повтор после переключения primary."),
        PostgresException postgresException when IsSlotInvalidated(postgresException) =>
            new(true, $"Слот инвалидирован сервером (SQLSTATE {postgresException.SqlState}: {postgresException.MessageText}) — WAL утерян, чтение невозможно."),
        PostgresException postgresException =>
            new(false, $"PostgreSQL отклонил сессию репликации (SQLSTATE {postgresException.SqlState}: {postgresException.MessageText})."),
        NpgsqlException or IOException =>
            new(false, "Сетевая ошибка или недоступность PostgreSQL — повторное подключение после паузы."),
        FormatException or InvalidOperationException =>
            new(false, "Ошибка декодирования строки outbox — структура таблицы не соответствует ожидаемым колонкам; сообщение будет перечитано с той же позиции."),
        _ => new(false, "Неожиданная ошибка чтения слота."),
    };

    private async Task HandleCommitAsync(
        CommitMessage commit,
        List<OutboxMessage> transactionMessages,
        ChannelWriter<OutboxMessage> output,
        CancellationToken cancellationToken)
    {
        if (transactionMessages.Count > 0)
        {
            // транзакция с outbox-сообщениями — активность потока (сброс таймера простоя)
            _idleTracker.MarkActivity();

            // LSN конца транзакции известен только в Commit — штамп последнему сообщению
            transactionMessages[^1].TransactionEndLsn = (ulong)commit.TransactionEndLsn;

            // запись пачки: заполненный канал останавливает чтение — backpressure
            foreach (var buffered in transactionMessages)
            {
                await output.WriteAsync(buffered, cancellationToken);
                metrics.RegisterReadMessage();
            }

            logger.LogInformation(
                "Транзакция {CommitLsn}: декодировано {MessageCount} outbox-сообщений.",
                commit.TransactionEndLsn,
                transactionMessages.Count);
            transactionMessages.Clear();
        }

        metrics.TouchIteration();
    }

    private async Task StopGracefullyAsync(ChannelWriter<OutboxMessage> output, LogicalReplicationConnection connection)
    {
        // канал завершает единственный писатель — публикатор дренирует хвост
        // и сохраняет финальный checkpoint, не дожидаясь процессора
        output.TryComplete();
        await WaitForFinalCheckpointAsync();
        await SendFinalStatusUpdateAsync(connection);
    }

    private async Task WaitForFinalCheckpointAsync()
    {
        var deadline = timeProvider.GetUtcNow() + _finalCheckpointWait;
        while (progress.CheckpointedPosition < progress.DeliveredPosition)
        {
            var remaining = deadline - timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                logger.LogWarning(
                    "Финальный checkpoint не сохранён за {WaitSeconds} с — подтверждаю серверу последнюю сохранённую позицию {Lsn}.",
                    _finalCheckpointWait.TotalSeconds,
                    LsnText.Format(progress.CheckpointedPosition));
                return;
            }

            // сигнал освобождается сохранением checkpoint; выход — по условию или таймауту
            await progress.WaitForCheckpointAsync(remaining, CancellationToken.None);
        }
    }

    private async Task SendFinalStatusUpdateAsync(LogicalReplicationConnection connection)
    {
        var target = progress.CheckpointedPosition;
        if (target == 0UL)
        {
            return;
        }

        try
        {
            connection.SetReplicationStatus(new NpgsqlLogSequenceNumber(target));
            await connection.SendStatusUpdate(CancellationToken.None);
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "Финальное подтверждение позиции {Lsn} не доставлено серверу — при следующем старте сработает защита согласования checkpoint и слота.",
                LsnText.Format(target));
        }
    }

    // цикл подтверждения позиции слота: отправляет серверу LSN после каждого сохранённого checkpoint
    private async Task FlushCheckpointAsync(LogicalReplicationConnection connection, CancellationToken cancellationToken)
    {
        // последняя позиция, отправленная серверу в этой сессии
        var lastSent = 0UL;
        while (!cancellationToken.IsCancellationRequested)
        {
            // сигнал нового checkpoint или тик интервала
            bool checkpointed;
            try
            {
                checkpointed = await progress.WaitForCheckpointAsync(_flushInterval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            var target = progress.CheckpointedPosition;
            if (checkpointed && target > lastSent)
            {
                try
                {
                    // двигаем подтверждённую позицию слота: сам Npgsql выталкивает только received
                    connection.SetReplicationStatus(new NpgsqlLogSequenceNumber(target));
                    await connection.SendStatusUpdate(CancellationToken.None);
                    lastSent = target;
                }
                catch (Exception exception)
                {
                    logger.LogDebug(
                        exception,
                        "Отправка подтверждения позиции {Lsn} не прошла: позиция уйдёт следующим обновлением статуса.",
                        LsnText.Format(target));
                }
            }

            RefreshCheckpointMetrics();
        }
    }

    // цикл idle-предупреждений: WARN, если поток без новых сообщений дольше таймаута
    private async Task IdleWarnLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (_idleTracker.TryBeginWarning(_idleTimeout) is { } idle)
            {
                logger.LogWarning(
                    "Простой потока {IdleSeconds:N0} с (порог {IdleTimeoutSeconds:N0} с): новые outbox-сообщения не поступают, слот не подтверждается — WAL может накапливаться. Проверь запись в outbox.",
                    idle.TotalSeconds,
                    _idleTimeout.TotalSeconds);
            }
        }
    }

    private void RefreshCheckpointMetrics()
    {
        if (progress.LastCheckpointAtUtc is { } checkpointedAt)
        {
            metrics.SetCheckpointAge(timeProvider.GetUtcNow().UtcDateTime - checkpointedAt);
        }

        metrics.SetCheckpointLagBytes((long)(progress.ReadPosition - progress.CheckpointedPosition));
    }

    private static async Task SwallowAsync(Task background)
    {
        try
        {
            await background;
        }
        catch (Exception)
        {
            // гасим ошибку фоновой задачи: наружу должен выйти результат чтения
        }
    }

    private async Task TryReadInsertAsync(
        InsertMessage insert,
        Dictionary<uint, OutboxColumnPlan> plans,
        HashSet<string> warnedRelations,
        List<OutboxMessage> transactionMessages,
        CancellationToken cancellationToken)
    {
        var relation = insert.Relation;
        if (!IsOutboxRelation(relation.Namespace, relation.RelationName, _schema, _table))
        {
            await ConsumeTupleAsync(insert.NewRow, cancellationToken);
            if (warnedRelations.Add($"{relation.Namespace}.{relation.RelationName}"))
            {
                logger.LogWarning(
                    "{Warning}",
                    FindForeignRelationWarning(relation.Namespace, relation.RelationName, _publicationName));
            }

            return;
        }

        if (!plans.TryGetValue(relation.RelationId, out var plan))
        {
            var columnNames = new string[relation.Columns.Count];
            var dataTypeIds = new uint[relation.Columns.Count];
            for (var i = 0; i < relation.Columns.Count; i++)
            {
                columnNames[i] = relation.Columns[i].ColumnName;
                dataTypeIds[i] = relation.Columns[i].DataTypeId;
            }

            plan = OutboxRowParser.BuildColumnPlan(columnNames, dataTypeIds);
            if (_binary)
            {
                OutboxRowParser.EnsureBinaryTypes(plan, columnNames);
            }

            plans.Add(relation.RelationId, plan);
        }

        if (_binary)
        {
            await ReadBinaryInsertAsync(insert, relation, plan, transactionMessages, cancellationToken);
        }
        else
        {
            await ReadTextInsertAsync(insert, relation, plan, transactionMessages, cancellationToken);
        }
    }

    private static async Task ReadTextInsertAsync(
        InsertMessage insert,
        RelationMessage relation,
        OutboxColumnPlan plan,
        List<OutboxMessage> transactionMessages,
        CancellationToken cancellationToken)
    {
        string? id = null;
        string? topic = null;
        string? key = null;
        string? body = null;
        string? headers = null;
        string? partition = null;
        string? producerKey = null;
        string? createdAt = null;

        var index = 0;
        await foreach (var value in insert.NewRow)
        {
            var slot = plan.Slots[index++];
            if (slot == OutboxColumnSlot.Ignored)
            {
                continue;
            }

            if (value.Kind == TupleDataKind.Null)
            {
                continue;
            }

            if (value.Kind != TupleDataKind.TextValue)
            {
                throw UnexpectedValueKind(relation, index, value.Kind, "текст");
            }

            var text = await value.Get<string>(cancellationToken);
            switch (slot)
            {
                case OutboxColumnSlot.Id: id = text; break;
                case OutboxColumnSlot.Topic: topic = text; break;
                case OutboxColumnSlot.Key: key = text; break;
                case OutboxColumnSlot.Body: body = text; break;
                case OutboxColumnSlot.Headers: headers = text; break;
                case OutboxColumnSlot.Partition: partition = text; break;
                case OutboxColumnSlot.ProducerKey: producerKey = text; break;
                case OutboxColumnSlot.CreatedAt: createdAt = text; break;
            }
        }

        var message = OutboxRowParser.Parse(id, topic, key, body, headers, partition, producerKey, createdAt);
        transactionMessages.Add(message);
    }

    private static async Task ReadBinaryInsertAsync(
        InsertMessage insert,
        RelationMessage relation,
        OutboxColumnPlan plan,
        List<OutboxMessage> transactionMessages,
        CancellationToken cancellationToken)
    {
        byte[]? id = null;
        byte[]? topic = null;
        byte[]? key = null;
        byte[]? body = null;
        byte[]? headers = null;
        byte[]? partition = null;
        byte[]? producerKey = null;
        byte[]? createdAt = null;

        var index = 0;
        await foreach (var value in insert.NewRow)
        {
            var slot = plan.Slots[index++];
            if (slot == OutboxColumnSlot.Ignored)
            {
                continue;
            }

            if (value.Kind == TupleDataKind.Null)
            {
                continue;
            }

            if (value.Kind != TupleDataKind.BinaryValue)
            {
                throw UnexpectedValueKind(relation, index, value.Kind, "бинарное представление");
            }

            var bytes = await ReadBinaryValueAsync(value, cancellationToken);
            switch (slot)
            {
                case OutboxColumnSlot.Id: id = bytes; break;
                case OutboxColumnSlot.Topic: topic = bytes; break;
                case OutboxColumnSlot.Key: key = bytes; break;
                case OutboxColumnSlot.Body: body = bytes; break;
                case OutboxColumnSlot.Headers: headers = bytes; break;
                case OutboxColumnSlot.Partition: partition = bytes; break;
                case OutboxColumnSlot.ProducerKey: producerKey = bytes; break;
                case OutboxColumnSlot.CreatedAt: createdAt = bytes; break;
            }
        }

        var message = OutboxRowParser.ParseBinary(id, topic, key, body, headers, partition, producerKey, createdAt);
        transactionMessages.Add(message);
    }

    private static FormatException UnexpectedValueKind(RelationMessage relation, int index, TupleDataKind actualKind, string expected)
    {
        return new FormatException(
            $"Колонка {relation.Columns[index - 1].ColumnName}: значение пришло видом {actualKind}, ожидается {expected}.");
    }

    private static async Task<byte[]> ReadBinaryValueAsync(ReplicationValue value, CancellationToken cancellationToken)
    {
        var bytes = new byte[value.Length];
        await using var stream = value.GetStream();
        await stream.ReadExactlyAsync(bytes, cancellationToken);
        return bytes;
    }

    private static async Task ConsumeTupleAsync(ReplicationTuple tuple, CancellationToken cancellationToken)
    {
        // значения кортежа читаются из стрима лениво, при переборе: полностью
        // вычитываем, иначе его байты сломают разбор следующих сообщений
        await foreach (var _ in tuple.WithCancellation(cancellationToken))
        {
        }
    }

    private static bool IsSlotInvalidated(PostgresException exception) =>
        exception.MessageText.Contains("invalidated", StringComparison.OrdinalIgnoreCase);
}
