namespace CdcOutbox.Core.Processing;

/// <summary>
/// Действие Recovery Mode по результату сопоставления сохранённого checkpoint
/// с состоянием слота.
/// </summary>
public enum RecoveryAction
{
    /// <summary>
    /// Восстановление не требуется: согласование пройдёт штатно (первый запуск
    /// или checkpoint не опережает слот).
    /// </summary>
    NotNeeded,

    /// <summary>
    /// Принять позицию существующего слота (confirmed_flush) как новый checkpoint;
    /// окно между позициями пропускается.
    /// </summary>
    AdoptSlotPosition,

    /// <summary>
    /// Создать отсутствующий слот с текущей позиции WAL и принять её как новый
    /// checkpoint; всё до позиции создания пропускается.
    /// </summary>
    CreateSlot,

    /// <summary>
    /// Продолжение невозможно — требуется ручное вмешательство до перезапуска.
    /// </summary>
    Reject,
}

/// <summary>
/// Решение Recovery Mode.
/// </summary>
/// <param name="Action">Действие.</param>
/// <param name="Description">Пояснение: для <see cref="RecoveryAction.Reject"/> — текст ошибки, иначе комментарий для журнала.</param>
public sealed record RecoveryDecision(RecoveryAction Action, string Description);

/// <summary>
/// Правила выбора действия Recovery Mode по состоянию checkpoint-стора и слота.
/// </summary>
public static class RecoveryPlanner
{
    /// <summary>
    /// Решить, как восстанавливать доставку при известном сохранённом checkpoint
    /// и состоянии слота.
    /// </summary>
    /// <param name="checkpoint">Последний подтверждённо сохранённый checkpoint; null, если сохранений не было.</param>
    /// <param name="slotExists">Существует ли слот.</param>
    /// <param name="confirmedFlush">Позиция confirmed_flush слота; null, если слота нет или confirmed_flush пуст.</param>
    /// <returns>Решение: действие и пояснение.</returns>
    public static RecoveryDecision Plan(ulong? checkpoint, bool slotExists, ulong? confirmedFlush)
    {
        if (checkpoint is null)
        {
            return new(
                RecoveryAction.NotNeeded,
                "сохранённого checkpoint нет — согласование пройдёт штатно");
        }

        if (!slotExists)
        {
            return new(
                RecoveryAction.CreateSlot,
                "слот отсутствует — создастся с текущей позиции WAL, недоставленное окно пропускается");
        }

        if (confirmedFlush is not { } flush)
        {
            return new(
                RecoveryAction.Reject,
                "Слот существует, но confirmed_flush пуст — состояние слота непригодно для восстановления. " +
                "Удали слот вручную и перезапусти сервис с Recovery Mode: новый слот начнётся с текущей позиции WAL.");
        }

        if (checkpoint.Value <= flush)
        {
            return new(
                RecoveryAction.NotNeeded,
                "checkpoint не опережает confirmed_flush слота — рассогласования нет, продолжение штатное");
        }

        return new(
            RecoveryAction.AdoptSlotPosition,
            "checkpoint опережает confirmed_flush слота — принимаю позицию слота, окно между позициями пропускается");
    }
}
