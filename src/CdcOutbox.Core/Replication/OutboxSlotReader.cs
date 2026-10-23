using System.Threading.Channels;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace CdcOutbox.Core.Replication;

/// <summary>
/// Чтение failover-слота логической репликации: START_REPLICATION pgoutput
/// (протокол v1), декодирование INSERT outbox-таблицы в <see cref="OutboxMessage"/>,
/// запись в bounded-канал (заполнение канала останавливает чтение — backpressure).
/// Позиция слота не подтверждается: без подтверждённой доставки в Kafka
/// продвижение позиции означало бы потерю сообщений (at-least-once) —
/// после переподключения чтение продолжится с confirmed_flush слота.
/// </summary>
public sealed class OutboxSlotReader : IOutboxSlotReader
{
    private readonly string _connectionString;
    private readonly string _slotName;
    private readonly string _publicationName;
    private readonly string _schema;
    private readonly string _table;
    private readonly OutboxMetrics _metrics;
    private readonly ILogger<OutboxSlotReader> _logger;

    /// <summary>
    /// Читатель на параметрах конфигурации сервиса.
    /// </summary>
    /// <param name="postgresOptions">Подключение к PostgreSQL.</param>
    /// <param name="replicationOptions">Слот и publication.</param>
    /// <param name="storageOptions">Схема и таблица outbox.</param>
    /// <param name="metrics">Метрики процессора.</param>
    /// <param name="logger">Журнал.</param>
    public OutboxSlotReader(
        IOptions<PostgresOptions> postgresOptions,
        IOptions<ReplicationOptions> replicationOptions,
        IOptions<OutboxStorageOptions> storageOptions,
        OutboxMetrics metrics,
        ILogger<OutboxSlotReader> logger)
    {
        _connectionString = postgresOptions.Value.ConnectionString;
        _slotName = replicationOptions.Value.SlotName;
        _publicationName = replicationOptions.Value.PublicationName;
        _schema = NormalizeIdentifier(storageOptions.Value.Schema);
        _table = NormalizeIdentifier(storageOptions.Value.Table);
        _metrics = metrics;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task ReadAsync(ChannelWriter<OutboxMessage> output, CancellationToken cancellationToken)
    {
        await using var connection = new LogicalReplicationConnection(_connectionString);
        await connection.Open(cancellationToken);

        var slot = new PgOutputReplicationSlot(_slotName);
        var options = new PgOutputReplicationOptions(_publicationName, PgOutputProtocolVersion.V1);
        var stream = connection.StartReplication(slot, options, cancellationToken);

        var plans = new Dictionary<uint, OutboxColumnPlan>();
        var warnedRelations = new HashSet<string>(StringComparer.Ordinal);
        var transactionMessages = 0;

        await foreach (var message in stream)
        {
            switch (message)
            {
                case InsertMessage insert:
                    if (await TryReadInsertAsync(insert, plans, warnedRelations, output, cancellationToken))
                    {
                        transactionMessages++;
                    }

                    break;
                case BeginMessage:
                    transactionMessages = 0;
                    break;
                case CommitMessage commit:
                    if (transactionMessages > 0)
                    {
                        _logger.LogInformation(
                            "Транзакция {CommitLsn}: декодировано {MessageCount} outbox-сообщений.",
                            commit.TransactionEndLsn,
                            transactionMessages);
                        transactionMessages = 0;
                    }

                    _metrics.TouchIteration();
                    break;
            }
        }
    }

    private async Task<bool> TryReadInsertAsync(
        InsertMessage insert,
        Dictionary<uint, OutboxColumnPlan> plans,
        HashSet<string> warnedRelations,
        ChannelWriter<OutboxMessage> output,
        CancellationToken cancellationToken)
    {
        var relation = insert.Relation;
        if (!IsOutboxRelation(relation.Namespace, relation.RelationName, _schema, _table))
        {
            await ConsumeTupleAsync(insert.NewRow, cancellationToken);
            if (warnedRelations.Add($"{relation.Namespace}.{relation.RelationName}"))
            {
                _logger.LogWarning(
                    "{Warning}",
                    FindForeignRelationWarning(relation.Namespace, relation.RelationName, _publicationName));
            }

            return false;
        }

        if (!plans.TryGetValue(relation.RelationId, out var plan))
        {
            var columnNames = new string[relation.Columns.Count];
            for (var i = 0; i < relation.Columns.Count; i++)
            {
                columnNames[i] = relation.Columns[i].ColumnName;
            }

            plan = OutboxRowParser.BuildColumnPlan(columnNames);
            plans.Add(relation.RelationId, plan);
        }

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
                throw new FormatException(
                    $"Колонка {relation.Columns[index - 1].ColumnName}: значение пришло видом {value.Kind}, ожидается текст (pgoutput в текстовом формате, binary не включён).");
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
        await output.WriteAsync(message, cancellationToken);
        _metrics.RegisterReadMessage();
        return true;
    }

    private static async Task ConsumeTupleAsync(ReplicationTuple tuple, CancellationToken cancellationToken)
    {
        await foreach (var _ in tuple.WithCancellation(cancellationToken))
        {
        }
    }

    internal static string NormalizeIdentifier(string identifier) => identifier.Trim().ToLowerInvariant();

    internal static bool IsOutboxRelation(string? @namespace, string? relationName, string schema, string table) =>
        string.Equals(@namespace, schema, StringComparison.Ordinal)
        && string.Equals(relationName, table, StringComparison.Ordinal);

    internal static string FindForeignRelationWarning(string @namespace, string relationName, string publicationName) =>
        $"В поток слота попала вставка чужой таблицы {@namespace}.{relationName} — проверь состав публикации '{publicationName}': кроме таблицы outbox (publish='insert') она ничего не должна публиковать.";

    internal static string ClassifyReadingIssue(Exception exception) => exception switch
    {
        PostgresException { SqlState: "55006" } postgresException =>
            $"Слот занят другим консюмером (SQLSTATE 55006: {postgresException.MessageText}) — повторное подключение после паузы.",
        PostgresException { SqlState: PostgresErrorCodes.UndefinedObject } =>
            "Слот отсутствует или удалён (SQLSTATE 42704) — автопересоздание запрещено: пересоздание с текущей позиции потеряет непрочитанный WAL. Требуется вмешательство оператора.",
        PostgresException { SqlState: PostgresErrorCodes.ReadOnlySqlTransaction } =>
            "Подключение попало на read-only узел (SQLSTATE 25006) — повтор после переключения primary.",
        PostgresException postgresException =>
            $"PostgreSQL отклонил сессию репликации (SQLSTATE {postgresException.SqlState}: {postgresException.MessageText}).",
        NpgsqlException or IOException =>
            "Сетевая ошибка или недоступность PostgreSQL — повторное подключение после паузы.",
        FormatException or InvalidOperationException =>
            "Ошибка декодирования строки outbox — структура таблицы не соответствует ожидаемым колонкам; сообщение будет перечитано с той же позиции.",
        _ => "Неожиданная ошибка чтения слота.",
    };
}
