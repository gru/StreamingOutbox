using CdcOutbox.Core.Replication;

namespace CdcOutbox.Core.Processing;

/// <summary>
/// Правила согласования сохранённого checkpoint с состоянием слота при старте.
/// </summary>
public static class CheckpointReconciler
{
    /// <summary>
    /// Проверить, безопасно ли продолжать доставку с позиции слота при известном
    /// сохранённом checkpoint.
    /// </summary>
    /// <param name="slotExists">Существует ли слот.</param>
    /// <param name="confirmedFlushDiffBytes">Разница confirmed_flush слота и checkpoint, байт (положительная — checkpoint позади); null, если confirmed_flush пуст.</param>
    /// <param name="checkpoint">Последний подтверждённо сохранённый checkpoint; null, если сохранений не было.</param>
    /// <returns>Описание проблемы; null, если продолжение безопасно.</returns>
    public static string? FindIssue(bool slotExists, long? confirmedFlushDiffBytes, ulong? checkpoint)
    {
        if (checkpoint is null)
        {
            return null;
        }

        if (!slotExists)
        {
            return
                "Слот отсутствует, а checkpoint подтверждённо сохранён: автопересоздание запрещено — " +
                "новый слот начнётся с текущей позиции WAL и потеряет недоставленное окно. " +
                "Восстанови слот вручную или очисти checkpoint-стор и перезапусти сервис.";
        }

        if (confirmedFlushDiffBytes is null)
        {
            return
                $"Checkpoint {LsnText.Format(checkpoint.Value)} не с чем сравнить: confirmed_flush слота пуст. " +
                "Состояния checkpoint-стора и слота разошлись — продолжение требует вмешательства оператора.";
        }

        if (confirmedFlushDiffBytes < 0)
        {
            return
                $"Checkpoint {LsnText.Format(checkpoint.Value)} опережает confirmed_flush слота " +
                $"на {-confirmedFlushDiffBytes.Value} байт: состояния checkpoint-стора и слота разошлись " +
                "(слот пересоздан или отмотан, либо checkpoint остался от другого экземпляра стенда); " +
                "WAL между позициями мог быть переработан — продолжение требует вмешательства оператора.";
        }

        return null;
    }
}
