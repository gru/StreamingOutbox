namespace Writer.Configuration;

/// <summary>
/// Режим записи батчей в PostgreSQL.
/// </summary>
public enum WriterWriteMode
{
    /// <summary>
    /// Штатный режим стенда: INSERT + DELETE одной транзакцией в outbox.messages.
    /// </summary>
    InsertDelete = 0,

    /// <summary>
    /// Только INSERT в секционированную таблицу-страховку без DELETE (очистка через DROP TABLE часовых секций).
    /// </summary>
    InsertOnly = 1,

    /// <summary>
    /// Целевой безтабличный режим: упаковка батча в OBX1 SuperFrame и запись напрямую в WAL (outbox.emit_frame).
    /// </summary>
    WalEmit = 2,
}
