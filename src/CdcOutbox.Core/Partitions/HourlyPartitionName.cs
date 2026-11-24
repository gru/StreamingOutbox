using System.Globalization;

namespace CdcOutbox.Core.Partitions;

/// <summary>
/// Детерминированный расчёт имён, границ и расписания обслуживания почасовых партиций (UTC).
/// </summary>
public static class HourlyPartitionName
{
    /// <summary>
    /// Округлить метку времени вниз до начала часа в UTC.
    /// </summary>
    /// <param name="timestampUtc">Исходная метка времени.</param>
    /// <returns>Начало часа в UTC.</returns>
    public static DateTimeOffset TruncateToHourUtc(DateTimeOffset timestampUtc)
    {
        var utc = timestampUtc.ToUniversalTime();
        return new DateTimeOffset(utc.Year, utc.Month, utc.Day, utc.Hour, 0, 0, TimeSpan.Zero);
    }

    /// <summary>
    /// Сформировать имя часовой партиции вида &lt;parent&gt;_YYYYMMDD_HH.
    /// </summary>
    /// <param name="parentTable">Имя родительской таблицы.</param>
    /// <param name="hourStartUtc">Начало часа в UTC.</param>
    /// <returns>Имя дочерней партиции в нижнем регистре.</returns>
    public static string FormatPartitionName(string parentTable, DateTimeOffset hourStartUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(parentTable);
        var normalized = parentTable.Trim().ToLowerInvariant();
        var utc = TruncateToHourUtc(hourStartUtc);
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{normalized}_{utc:yyyyMMdd_HH}");
    }

    /// <summary>
    /// Построить список всех часовых партиций от текущего часа на <paramref name="precreateHours"/> часов вперёд.
    /// </summary>
    /// <param name="parentTable">Имя родительской таблицы.</param>
    /// <param name="nowUtc">Текущее время UTC.</param>
    /// <param name="precreateHours">Число будущих часов про запас (например, 48 = 2 дня вперёд).</param>
    /// <returns>Упорядоченный список окон: текущий час + <paramref name="precreateHours"/> будущих часов.</returns>
    public static IReadOnlyList<HourlyPartitionWindow> BuildHorizon(
        string parentTable,
        DateTimeOffset nowUtc,
        int precreateHours)
    {
        if (precreateHours < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(precreateHours), "Горизонт предсоздания должен быть >= 1 часа.");
        }

        var currentHour = TruncateToHourUtc(nowUtc);
        var windows = new List<HourlyPartitionWindow>(precreateHours + 1);
        for (var offsetHours = 0; offsetHours <= precreateHours; offsetHours++)
        {
            var start = currentHour.AddHours(offsetHours);
            var end = start.AddHours(1);
            windows.Add(new HourlyPartitionWindow(
                FormatPartitionName(parentTable, start),
                start,
                end));
        }

        return windows;
    }

    /// <summary>
    /// Вычислить точное время следующего планового запуска в HH:<paramref name="runAtMinuteOfHour"/>:00 UTC
    /// строго позже <paramref name="nowUtc"/> (без дрейфа расписания).
    /// </summary>
    /// <param name="nowUtc">Текущее время UTC.</param>
    /// <param name="runAtMinuteOfHour">Минута часа запуска [0..59] (по умолчанию 5 → HH:05:00 UTC).</param>
    /// <returns>Следующая точка запуска в UTC.</returns>
    public static DateTimeOffset CalculateNextRunUtc(DateTimeOffset nowUtc, int runAtMinuteOfHour)
    {
        if (runAtMinuteOfHour is < 0 or > 59)
        {
            throw new ArgumentOutOfRangeException(nameof(runAtMinuteOfHour), "Минута часа должна быть в диапазоне [0..59].");
        }

        var utc = nowUtc.ToUniversalTime();
        var currentHourTarget = new DateTimeOffset(
            utc.Year,
            utc.Month,
            utc.Day,
            utc.Hour,
            runAtMinuteOfHour,
            0,
            TimeSpan.Zero);

        return utc < currentHourTarget ? currentHourTarget : currentHourTarget.AddHours(1);
    }
}
