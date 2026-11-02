using System.Globalization;

namespace CdcOutbox.Core.Replication;

/// <summary>
/// Текстовая форма LSN PostgreSQL «X/Y» (обе части — шестнадцатеричные).
/// </summary>
public static class LsnText
{
    private const int PartBits = 32;
    private const ulong MaxPart = (1UL << PartBits) - 1;

    /// <summary>
    /// Разобрать текстовую форму LSN «X/Y» (шестнадцатеричные части,
    /// допустимы ведущие нули).
    /// </summary>
    /// <param name="text">Текст в форме «X/Y».</param>
    /// <returns>Числовое значение LSN; null, если текст не является корректным LSN.</returns>
    public static ulong? TryParse(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        var slash = text.IndexOf('/');
        if (slash <= 0 || slash == text.Length - 1)
        {
            return null;
        }

        // старшая часть — hex до «/» (AsSpan — разбор без аллокации подстроки)
        if (!ulong.TryParse(text.AsSpan(0, slash), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var high))
        {
            return null;
        }

        // младшая часть — hex после «/»
        if (!ulong.TryParse(text.AsSpan(slash + 1), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var low))
        {
            return null;
        }

        // в pg_lsn каждая часть — ровно 32 бита: шире — не pg_lsn
        if (high > MaxPart || low > MaxPart)
        {
            return null;
        }

        return CombineParts(high, low);
    }

    /// <summary>
    /// Сформировать текстовую форму LSN без дополнения нулями.
    /// </summary>
    /// <param name="lsn">Числовое значение LSN.</param>
    /// <returns>Текст в форме «X/Y».</returns>
    public static string Format(ulong lsn)
    {
        var upperPart = UpperPart(lsn).ToString("X", CultureInfo.InvariantCulture);
        var lowerPart = LowerPart(lsn).ToString("X", CultureInfo.InvariantCulture);
        return $"{upperPart}/{lowerPart}";
    }

    private static ulong UpperPart(ulong lsn) => lsn >> PartBits;

    private static ulong LowerPart(ulong lsn) => lsn & MaxPart;

    private static ulong CombineParts(ulong upperPart, ulong lowerPart) => (upperPart << PartBits) | lowerPart;
}
