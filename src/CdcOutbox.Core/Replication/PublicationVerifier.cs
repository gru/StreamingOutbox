using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Processing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CdcOutbox.Core.Replication;

/// <summary>
/// Проверяет publication: публикует INSERT и содержит outbox-таблицу.
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
    public async Task VerifyAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_postgres.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        var state = await ReadStateAsync(connection, cancellationToken);
        var issue = FindPublicationIssue(state, _replication.PublicationName, _storage.Schema, _storage.Table);
        if (issue is not null)
        {
            throw new StartupCheckException(issue);
        }

        var warning = FindExtraOperationsWarning(state, _replication.PublicationName);
        if (warning is not null)
        {
            logger.LogWarning("{Warning}", warning);
        }

        logger.LogInformation(
            "Publication '{PublicationName}' публикует INSERT таблицы {Schema}.{Table}.",
            _replication.PublicationName, _storage.Schema, _storage.Table);
    }

    /// <summary>
    /// Возвращает описание проблемы несоответствия publication потребностям сервиса:
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

        await using var publicationCommand = new NpgsqlCommand(
            "SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = $1",
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
            }
        }

        if (!exists)
        {
            return new PublicationState(false, false, false, false, false, false);
        }

        await using var tableCommand = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3)",
            connection);
        tableCommand.Parameters.Add(new() { Value = _replication.PublicationName });
        tableCommand.Parameters.Add(new() { Value = _storage.Schema });
        tableCommand.Parameters.Add(new() { Value = _storage.Table });

        var containsOutboxTable = await tableCommand.ExecuteScalarAsync(cancellationToken) is true;

        return new PublicationState(
            exists, publishesInserts, publishesUpdates, publishesDeletes, publishesTruncates, containsOutboxTable);
    }
}
