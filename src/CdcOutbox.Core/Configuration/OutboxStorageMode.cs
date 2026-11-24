namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Режим источника событий outbox.
/// </summary>
public enum OutboxStorageMode
{
    /// <summary>
    /// Штатный табличный режим (существующая таблица outbox без управления партициями).
    /// </summary>
    Table = 0,

    /// <summary>
    /// Целевой безтабличный режим: доставка сообщений напрямую из WAL
    /// (pg_logical_emit_message + pgoutput messages=true).
    /// </summary>
    WalMessages = 1,

    /// <summary>
    /// Режим со страховочной почасовой секционированной таблицей:
    /// INSERT в партиции + чтение из WAL + автонарезка наперёд, удаление после сдвига LSN
    /// и 24/7 авто-восстановление при потере слота.
    /// </summary>
    PartitionedTable = 2,
}
