namespace CdcOutbox.Core.Processing;

/// <summary>
/// Стартовые проверки при получении лидерства (см. <see cref="OutboxStartupChecks"/>):
/// выполняются арбитражем Active-Passive только после получения лидерства.
/// </summary>
public interface IOutboxStartupChecks
{
    /// <summary>
    /// Прогнать проверки; ошибка останавливает хост.
    /// </summary>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Задача проверок.</returns>
    Task RunAllAsync(CancellationToken cancellationToken);
}
