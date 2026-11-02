using CdcOutbox.Core.Kafka;

namespace CdcOutbox.IntegrationTests;

/// <summary>
/// Ожидание условий с поллингом и проглатывание ожидаемых исключений в интеграционных тестах.
/// </summary>
internal static class Wait
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// Дождаться истинности асинхронного условия до истечения таймаута.
    /// </summary>
    /// <param name="condition">Условие.</param>
    /// <param name="timeout">Бюджет ожидания.</param>
    /// <returns>True, если условие выполнено.</returns>
    public static async Task<bool> UntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(PollInterval);
        }

        return await condition();
    }

    /// <summary>
    /// Дождаться истинности синхронного условия до истечения таймаута.
    /// </summary>
    /// <param name="condition">Условие.</param>
    /// <param name="timeout">Бюджет ожидания.</param>
    /// <returns>True, если условие выполнено.</returns>
    public static async Task<bool> UntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(PollInterval);
        }

        return condition();
    }

    /// <summary>
    /// Дождаться checkpoint из топика; null, если за таймаут не появился.
    /// </summary>
    /// <param name="store">Checkpoint-стор.</param>
    /// <param name="greaterThan">Дождаться позиции строго больше этой.</param>
    /// <param name="timeout">Бюджет ожидания.</param>
    /// <returns>Сохранённая позиция; null по таймауту.</returns>
    public static async Task<ulong?> UntilCheckpointAsync(
        CheckpointStore store,
        ulong? greaterThan = null,
        TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        while (DateTime.UtcNow < deadline)
        {
            var checkpoint = store.ReadLatest(CancellationToken.None);
            if (checkpoint is { } value && (greaterThan is null || value > greaterThan))
            {
                return value;
            }

            await Task.Delay(PollInterval);
        }

        return null;
    }

    /// <summary>
    /// Проглотить любое исключение задачи (ожидаемое завершение по отмене и т.п.).
    /// </summary>
    /// <param name="task">Задача.</param>
    public static async Task SwallowAsync(Task task)
    {
        try
        {
            await task;
        }
        catch (Exception)
        {
        }
    }
}
