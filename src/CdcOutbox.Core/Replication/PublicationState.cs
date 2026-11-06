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
