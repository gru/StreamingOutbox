namespace CdcOutbox.Core.Processing;

/// <summary>
/// Решение Recovery Mode.
/// </summary>
/// <param name="Action">Действие.</param>
/// <param name="Description">Пояснение: для <see cref="RecoveryAction.Reject"/> — текст ошибки, иначе комментарий для журнала.</param>
public sealed record RecoveryDecision(RecoveryAction Action, string Description);
