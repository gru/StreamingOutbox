using System.Threading.Channels;

namespace CdcOutbox.Core.Replication;

/// <summary>
/// Читатель failover-слота: одно обращение — одна сессия стриминга
/// до ошибки или отмены; повторное подключение выполняет процессор.
/// </summary>
public interface IOutboxSlotReader
{
    /// <summary>
    /// Читать слот (pgoutput, протокол v1) и декодированные INSERT outbox-таблицы
    /// записывать в канал. Позиция слота не подтверждается.
    /// </summary>
    /// <param name="output">Писатель bounded-канала сообщений.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Задача чтения.</returns>
    Task ReadAsync(ChannelWriter<OutboxMessage> output, CancellationToken cancellationToken);
}
