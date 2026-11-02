namespace CdcOutbox.Core.Replication;

/// <summary>
/// Классификация ошибки чтения слота: описание для журнала и признак,
/// имеет ли смысл повторять попытки.
/// </summary>
/// <param name="IsTerminal">Признак терминальной ошибки: повторные попытки бессмысленны.</param>
/// <param name="Description">Описание проблемы для журнала.</param>
internal readonly record struct ReadingIssue(bool IsTerminal, string Description);
