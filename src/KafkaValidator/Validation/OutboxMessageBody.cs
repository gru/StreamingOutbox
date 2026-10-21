using System.Globalization;
using System.Text.Json;

namespace KafkaValidator.Validation;

/// <summary>
/// Тестовое тело сообщения, разобранное из записи Kafka.
/// Идентичность — id (= bigint id строки outbox.messages); группировка прогонов — runId.
/// </summary>
public sealed record ConsumedMessage(long Id, Guid? RunId, string RawJson, DateTimeOffset TimestampUtc);

/// <summary>
/// Строгий разбор body записи Kafka — UTF-8 JSON {"id","runId","eventType","payload","timestamp"}
/// (контракт Writer↔KafkaValidator, outbox-table.md; общий код между сервисами не используется,
/// формат дублируется сознательно). Обязательны id (integer) и timestamp (UTC ISO-8601);
/// отсутствующий runId — не ошибка (запись уйдёт в skipped); присутствующий, но не-Guid — ошибка.
/// Любая структурная проблема → false.
/// </summary>
public static class OutboxMessageBody
{
    /// <summary>
    /// Разобрать body записи; false — любая структурная проблема (детали — класс).
    /// </summary>
    public static bool TryParse(ReadOnlySpan<byte> body, out ConsumedMessage? message)
    {
        message = null;
        try
        {
            using var doc = JsonDocument.Parse(body.ToArray());
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return false;
            if (root.TryGetProperty("id", out var idEl) is false || idEl.ValueKind != JsonValueKind.Number)
                return false;
            if (idEl.TryGetInt64(out var id) is false)
                return false;

            Guid? runId = null;
            if (root.TryGetProperty("runId", out var runEl))
            {
                if (runEl.ValueKind != JsonValueKind.String || Guid.TryParse(runEl.GetString(), out var parsed) is false)
                    return false;
                runId = parsed;
            }

            if (root.TryGetProperty("timestamp", out var tsEl) is false || tsEl.ValueKind != JsonValueKind.String)
                return false;
            // Writer пишет "yyyy-MM-dd'T'HH:mm:ss.fff'Z'"; общий фолбэк — на случай
            // чужих/будущих форматов, чтобы валидатор не терял корректные записи
            var tsStyles = DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;
            var tsText = tsEl.GetString()!;
            if (DateTimeOffset.TryParseExact(tsText, "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",
                    CultureInfo.InvariantCulture, tsStyles, out var ts) is false
                && DateTimeOffset.TryParse(tsText, CultureInfo.InvariantCulture, tsStyles, out ts) is false)
            {
                return false;
            }

            message = new ConsumedMessage(id, runId, root.GetRawText(), ts);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
