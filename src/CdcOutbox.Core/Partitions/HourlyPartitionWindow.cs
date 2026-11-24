namespace CdcOutbox.Core.Partitions;

/// <summary>
/// Описание одной часовой партиции таблицы outbox.
/// </summary>
/// <param name="PartitionName">Имя дочерней таблицы-партиции (без схемы).</param>
/// <param name="RangeStartUtc">Нижняя граница диапазона created_at (включительно, UTC).</param>
/// <param name="RangeEndUtc">Верхняя граница диапазона created_at (исключительно, UTC).</param>
public sealed record HourlyPartitionWindow(
    string PartitionName,
    DateTimeOffset RangeStartUtc,
    DateTimeOffset RangeEndUtc);
