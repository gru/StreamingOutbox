using System.Threading.Channels;

namespace CdcOutbox.Core.Replication;

/// <summary>
/// Интерфейс читателя слота репликации.
/// </summary>
public interface IOutboxSlotReader
{
    /// <summary>
    /// Читать слот до вызова отмены.
    /// </summary>
    /// <param name="output">Писатель bounded-канала сообщений.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Задача чтения.</returns>
    Task ReadAsync(ChannelWriter<OutboxMessage> output, CancellationToken cancellationToken);
}
