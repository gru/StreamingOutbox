namespace CdcOutbox.Core.Processing;

/// <summary>
/// Сигнал о том, что слот репликации инвалидирован во время работы,
/// но включено 24/7 авто-восстановление из почасовых партиций:
/// координатор лидерства перезапустит срок и дочитает пропущенное окно без остановки хоста.
/// </summary>
public sealed class SlotRecoveryRequiredException : Exception
{
    /// <summary>
    /// Создаёт исключение запроса авто-восстановления слота.
    /// </summary>
    /// <param name="message">Описание причины.</param>
    /// <param name="innerException">Исходное исключение PostgreSQL.</param>
    public SlotRecoveryRequiredException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
