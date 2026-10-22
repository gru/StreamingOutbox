using CdcOutbox.Core.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CdcOutbox.Core.Replication;

/// <summary>
/// Снимок состояния publication из каталогов pg_publication и pg_publication_tables.
/// </summary>
/// <param name="Exists">Publication существует.</param>
/// <param name="PublishesInserts">pubinsert.</param>
/// <param name="PublishesUpdates">pubupdate.</param>
/// <param name="PublishesDeletes">pubdelete.</param>
/// <param name="PublishesTruncates">pubtruncate.</param>
/// <param name="ContainsOutboxTable">Таблица outbox входит в состав publication.</param>
public sealed record PublicationState(
    bool Exists,
    bool PublishesInserts,
    bool PublishesUpdates,
    bool PublishesDeletes,
    bool PublishesTruncates,
    bool ContainsOutboxTable);

/// <summary>
/// Проверка publication PostgreSQL: имя (<see cref="ReplicationOptions.PublicationName"/>),
/// публикация INSERT и вхождение outbox-таблицы (<see cref="OutboxStorageOptions"/>);
/// публикация других операций — предупреждение.
/// </summary>
public sealed class PublicationVerifier
{
    private readonly PostgresOptions _postgres;
    private readonly ReplicationOptions _replication;
    private readonly OutboxStorageOptions _storage;
    private readonly ILogger<PublicationVerifier> _logger;

    /// <summary>
    /// Создать проверяльщик publication.
    /// </summary>
    /// <param name="postgres">Подключение к PostgreSQL.</param>
    /// <param name="replication">Имя publication.</param>
    /// <param name="storage">Схема и имя outbox-таблицы.</param>
    /// <param name="logger">Журнал.</param>
    public PublicationVerifier(
        IOptions<PostgresOptions> postgres,
        IOptions<ReplicationOptions> replication,
        IOptions<OutboxStorageOptions> storage,
        ILogger<PublicationVerifier> logger)
    {
        _postgres = postgres.Value;
        _replication = replication.Value;
        _storage = storage.Value;
        _logger = logger;
    }

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
            throw new InvalidOperationException(issue);
        }

        var warning = FindExtraOperationsWarning(state, _replication.PublicationName);
        if (warning is not null)
        {
            _logger.LogWarning("{Warning}", warning);
        }

        _logger.LogInformation(
            "Publication '{PublicationName}' публикует INSERT таблицы {Schema}.{Table}.",
            _replication.PublicationName, _storage.Schema, _storage.Table);
    }

    /// <summary>
    /// Проверить снимок состояния publication по потребностям сервиса:
    /// publication существует, публикует INSERT и содержит outbox-таблицу.
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
    /// Предупреждение о публикации операций помимо INSERT: они декодируются,
    /// но сервисом не обрабатываются.
    /// </summary>
    /// <param name="state">Снимок каталогов pg_publication*.</param>
    /// <param name="publicationName">Имя publication.</param>
    /// <returns>Текст предупреждения; null, если публикуется только INSERT.</returns>
    public static string? FindExtraOperationsWarning(PublicationState state, string publicationName)
    {
        if (!state.PublishesUpdates && !state.PublishesDeletes && !state.PublishesTruncates)
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

        {
            await using var command = new NpgsqlCommand(
                "SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = $1",
                connection);
            command.Parameters.Add(new() { Value = _replication.PublicationName });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            exists = await reader.ReadAsync(cancellationToken);
            if (exists)
            {
                publishesInserts = reader.GetBoolean(0);
                publishesUpdates = reader.GetBoolean(1);
                publishesDeletes = reader.GetBoolean(2);
                publishesTruncates = reader.GetBoolean(3);
            }
        }

        if (!exists)
        {
            return new PublicationState(false, false, false, false, false, false);
        }

        bool containsOutboxTable;
        {
            await using var command = new NpgsqlCommand(
                "SELECT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3)",
                connection);
            command.Parameters.Add(new() { Value = _replication.PublicationName });
            command.Parameters.Add(new() { Value = _storage.Schema });
            command.Parameters.Add(new() { Value = _storage.Table });

            var result = await command.ExecuteScalarAsync(cancellationToken);
            containsOutboxTable = result is true;
        }

        return new PublicationState(
            exists, publishesInserts, publishesUpdates, publishesDeletes, publishesTruncates, containsOutboxTable);
    }
}
