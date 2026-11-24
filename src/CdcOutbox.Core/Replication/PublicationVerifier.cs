using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Processing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CdcOutbox.Core.Replication;

/// <summary>
/// Проверяет publication: существование, публикацию INSERT, вхождение outbox-таблицы
/// и pubviaroot для секционированного режима.
/// </summary>
public sealed class PublicationVerifier(
    IOptions<PostgresOptions> postgres,
    IOptions<ReplicationOptions> replication,
    IOptions<OutboxStorageOptions> storage,
    ILogger<PublicationVerifier> logger)
{
    private readonly PostgresOptions _postgres = postgres.Value;
    private readonly ReplicationOptions _replication = replication.Value;
    private readonly OutboxStorageOptions _storage = storage.Value;

    /// <summary>
    /// Прочитать состояние publication из каталогов и сверить с потребностями сервиса;
    /// расхождение — исключение.
    /// </summary>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Задача проверки.</returns>
    public async Task VerifyAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_postgres.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        var state = await ReadStateAsync(connection, cancellationToken);
        var issue = FindPublicationIssue(state, _replication.PublicationName, _storage.Schema, _storage.Table, _storage.Mode);
        if (issue is not null)
        {
            throw new StartupCheckException(issue);
        }

        if (_storage.UsesTable)
        {
            var warning = FindExtraOperationsWarning(state, _replication.PublicationName);
            if (warning is not null)
            {
                logger.LogWarning("{Warning}", warning);
            }

            logger.LogInformation(
                "Publication '{PublicationName}' публикует INSERT таблицы {Schema}.{Table} (mode={Mode}, pubviaroot={ViaRoot}).",
                _replication.PublicationName,
                _storage.Schema,
                _storage.Table,
                _storage.Mode,
                state.PublishViaPartitionRoot);
        }
        else
        {
            logger.LogInformation(
                "Publication '{PublicationName}' найдена для безтабличного режима WalMessages (pgoutput messages=true, prefix='{Prefix}').",
                _replication.PublicationName,
                _storage.WalMessagePrefix);
        }
    }

    /// <summary>
    /// Возвращает описание проблемы несоответствия publication потребностям сервиса в табличном режиме:
    /// не существует, не публикует INSERT или не содержит outbox-таблицу.
    /// </summary>
    /// <param name="state">Снимок каталогов pg_publication*.</param>
    /// <param name="publicationName">Ожидаемое имя publication.</param>
    /// <param name="schemaName">Схема outbox-таблицы.</param>
    /// <param name="tableName">Имя outbox-таблицы.</param>
    /// <returns>Описание проблемы; null, если publication пригодна.</returns>
    public static string? FindPublicationIssue(
        PublicationState state,
        string publicationName,
        string schemaName,
        string tableName)
    {
        if (!state.Exists)
        {
            return $"Publication '{publicationName}' не найдена: создай publication с publish='insert' и таблицей {schemaName}.{tableName}.";
        }

        if (!state.PublishesInserts)
        {
            return $"Publication '{publicationName}' не публикует INSERT (pubinsert=f): события outbox не попадут в слот.";
        }

        if (!state.ContainsOutboxTable)
        {
            return $"Таблица {schemaName}.{tableName} не входит в publication '{publicationName}'.";
        }

        return null;
    }

    /// <summary>
    /// Возвращает описание проблемы несоответствия publication с учётом режима хранения outbox.
    /// </summary>
    /// <param name="state">Снимок каталогов pg_publication*.</param>
    /// <param name="publicationName">Ожидаемое имя publication.</param>
    /// <param name="schemaName">Схема outbox-таблицы.</param>
    /// <param name="tableName">Имя outbox-таблицы.</param>
    /// <param name="mode">Режим хранилища outbox.</param>
    /// <returns>Описание проблемы; null, если publication пригодна.</returns>
    public static string? FindPublicationIssue(
        PublicationState state,
        string publicationName,
        string schemaName,
        string tableName,
        OutboxStorageMode mode)
    {
        if (mode == OutboxStorageMode.WalMessages)
        {
            return state.Exists
                ? null
                : $"Publication '{publicationName}' не найдена: для режима WalMessages создай хотя бы пустую publication (CREATE PUBLICATION {publicationName}).";
        }

        var baseIssue = FindPublicationIssue(state, publicationName, schemaName, tableName);
        if (baseIssue is not null)
        {
            return baseIssue;
        }

        if (mode == OutboxStorageMode.PartitionedTable && !state.PublishViaPartitionRoot)
        {
            return $"Publication '{publicationName}' не имеет publish_via_partition_root=true (pubviaroot=f): " +
                   $"в режиме PartitionedTable включи ALTER PUBLICATION {publicationName} SET (publish_via_partition_root = true), " +
                   $"чтобы вставки в часовые секции приходили под именем родителя {schemaName}.{tableName}.";
        }

        return null;
    }

    /// <summary>
    /// Возвращает текст предупреждения о публикации операций помимо INSERT: они декодируются,
    /// но сервисом не обрабатываются.
    /// </summary>
    /// <param name="state">Снимок каталогов pg_publication*.</param>
    /// <param name="publicationName">Имя publication.</param>
    /// <returns>Текст предупреждения; null, если публикуется только INSERT.</returns>
    public static string? FindExtraOperationsWarning(PublicationState state, string publicationName)
    {
        if (state is { PublishesUpdates: false, PublishesDeletes: false, PublishesTruncates: false })
        {
            return null;
        }

        return $"Publication '{publicationName}' публикует не только INSERT " +
               $"(pubupdate={Format(state.PublishesUpdates)}, pubdelete={Format(state.PublishesDeletes)}, " +
               $"pubtruncate={Format(state.PublishesTruncates)}): эти операции для outbox-таблицы декодируются, " +
               "но не обрабатываются — рекомендуется publish='insert'.";
    }

    private static string Format(bool value) => value ? "t" : "f";

    private async Task<PublicationState> ReadStateAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        bool exists;
        var publishesInserts = false;
        var publishesUpdates = false;
        var publishesDeletes = false;
        var publishesTruncates = false;
        var publishViaPartitionRoot = false;

        await using var publicationCommand = new NpgsqlCommand(
            "SELECT pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot FROM pg_publication WHERE pubname = $1",
            connection);
        publicationCommand.Parameters.Add(new() { Value = _replication.PublicationName });

        // ридер закрывается до следующей команды: Npgsql не держит два ридера на соединении
        await using (var publicationReader = await publicationCommand.ExecuteReaderAsync(cancellationToken))
        {
            exists = await publicationReader.ReadAsync(cancellationToken);
            if (exists)
            {
                publishesInserts = publicationReader.GetBoolean(0);
                publishesUpdates = publicationReader.GetBoolean(1);
                publishesDeletes = publicationReader.GetBoolean(2);
                publishesTruncates = publicationReader.GetBoolean(3);
                publishViaPartitionRoot = publicationReader.GetBoolean(4);
            }
        }

        if (!exists)
        {
            return new PublicationState(false, false, false, false, false, false);
        }

        var containsOutboxTable = false;
        if (_storage.UsesTable)
        {
            await using var tableCommand = new NpgsqlCommand(
                "SELECT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3)",
                connection);
            tableCommand.Parameters.Add(new() { Value = _replication.PublicationName });
            tableCommand.Parameters.Add(new() { Value = _storage.Schema });
            tableCommand.Parameters.Add(new() { Value = _storage.Table });

            containsOutboxTable = await tableCommand.ExecuteScalarAsync(cancellationToken) is true;
        }

        return new PublicationState(
            exists,
            publishesInserts,
            publishesUpdates,
            publishesDeletes,
            publishesTruncates,
            containsOutboxTable,
            publishViaPartitionRoot);
    }
}
