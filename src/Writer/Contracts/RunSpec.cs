using Writer.Configuration;
using Writer.Generation;

namespace Writer.Contracts;

/// <summary>
/// Запрос запуска прогона (HTTP DTO для POST /api/generation/start); null = взять дефолт из конфигурации.
/// </summary>
public sealed class RunSpec
{
    /// <summary>
    /// Идентификатор прогона; null — сгенерировать новый Guid.
    /// </summary>
    public Guid? RunId { get; set; }

    /// <summary>
    /// Цель: суммарное число сообщений (>= 1); null — взять дефолт из конфигурации.
    /// </summary>
    public int? TotalMessages { get; set; }

    /// <summary>
    /// Цель: суммарный объём записи в байтах (>= 1), считается по размеру body;
    /// null — взять дефолт из конфигурации.
    /// </summary>
    public long? TotalPayloadBytes { get; set; }

    /// <summary>
    /// Цель: длительность прогона в секундах (>= 1); null — взять дефолт из конфигурации.
    /// </summary>
    public int? DurationSeconds { get; set; }

    /// <summary>
    /// true — генерировать до явного POST /api/generation/stop.
    /// </summary>
    public bool? RunUntilStopped { get; set; }

    /// <summary>
    /// Размер payload одного сообщения в байтах [1..10000000].
    /// </summary>
    public int? MessageSizeBytes { get; set; }

    /// <summary>
    /// Ограничение скорости, сообщений/с (>= 1); null — взять дефолт из конфигурации
    /// (там null = без ограничения).
    /// </summary>
    public int? RatePerSecond { get; set; }

    /// <summary>
    /// Максимум одновременно выполняемых батчей [1..64].
    /// </summary>
    public int? Parallelism { get; set; }

    /// <summary>
    /// Сообщений в одном батче [1..5000] (одна транзакция INSERT+DELETE).
    /// </summary>
    public int? BatchSize { get; set; }
}

/// <summary>
/// Разрешённый профиль прогона: все поля определены и провалидированы.
/// </summary>
public sealed record LoadProfile
{
    /// <summary>
    /// Идентификатор прогона (из запроса или сгенерированный).
    /// </summary>
    public required Guid RunId { get; init; }

    /// <summary>
    /// Размер payload одного сообщения в байтах.
    /// </summary>
    public required int MessageSizeBytes { get; init; }

    /// <summary>
    /// Ограничение скорости, сообщений/с; null — без ограничения.
    /// </summary>
    public required int? RatePerSecond { get; init; }

    /// <summary>
    /// Максимум одновременно выполняемых батчей.
    /// </summary>
    public required int Parallelism { get; init; }

    /// <summary>
    /// Сообщений в одном батче.
    /// </summary>
    public required int BatchSize { get; init; }

    /// <summary>
    /// Цель: суммарное число сообщений; null — цель не задана.
    /// </summary>
    public int? TotalMessages { get; init; }

    /// <summary>
    /// Цель: суммарный объём записи в байтах; null — цель не задана.
    /// </summary>
    public long? TotalPayloadBytes { get; init; }

    /// <summary>
    /// Цель: длительность прогона в секундах; null — цель не задана.
    /// </summary>
    public int? DurationSeconds { get; init; }

    /// <summary>
    /// true — генерировать до явного stop.
    /// </summary>
    public bool RunUntilStopped { get; init; }

    /// <summary>
    /// Сливает запрос с дефолтами конфигурации и валидирует; null + errors — некорректная комбинация.
    /// </summary>
    public static LoadProfile? Resolve(WriterOptions options, RunSpec? request, List<string> errors)
    {
        var d = options.Defaults;
        var r = request;
        var profile = new LoadProfile
        {
            RunId = r?.RunId ?? Guid.NewGuid(),
            MessageSizeBytes = r?.MessageSizeBytes ?? d.MessageSizeBytes,
            RatePerSecond = r?.RatePerSecond ?? d.RatePerSecond,
            Parallelism = r?.Parallelism ?? d.Parallelism,
            BatchSize = r?.BatchSize ?? d.BatchSize,
            TotalMessages = r?.TotalMessages ?? d.TotalMessages,
            TotalPayloadBytes = r?.TotalPayloadBytes ?? d.TotalPayloadBytes,
            DurationSeconds = r?.DurationSeconds ?? d.DurationSeconds,
            RunUntilStopped = r?.RunUntilStopped ?? d.RunUntilStopped,
        };

        if (profile.MessageSizeBytes is < 1 or > 10_000_000)
            errors.Add($"messageSizeBytes должен быть в [1..10000000], получено {profile.MessageSizeBytes}");
        if (profile.Parallelism is < 1 or > 64)
            errors.Add($"parallelism должен быть в [1..64], получено {profile.Parallelism}");
        if (profile.BatchSize is < 1 or > OutboxWriter.MaxBatchSize)
            errors.Add($"batchSize должен быть в [1..{OutboxWriter.MaxBatchSize}] (6 параметров на строку, лимит протокола 65535), получено {profile.BatchSize}");
        if (profile.RatePerSecond is < 1)
            errors.Add($"ratePerSecond должен быть >= 1 или отсутствовать (без лимита), получено {profile.RatePerSecond}");
        if (profile.DurationSeconds is < 1)
            errors.Add($"durationSeconds должен быть >= 1, получено {profile.DurationSeconds}");
        if (profile.TotalMessages is < 1)
            errors.Add($"totalMessages должен быть >= 1, получено {profile.TotalMessages}");
        if (profile.TotalPayloadBytes is < 1)
            errors.Add($"totalPayloadBytes должен быть >= 1, получено {profile.TotalPayloadBytes}");

        if (!profile.RunUntilStopped &&
            profile.TotalMessages is null &&
            profile.TotalPayloadBytes is null &&
            profile.DurationSeconds is null)
        {
            errors.Add("задайте хотя бы одну цель завершения (totalMessages / totalPayloadBytes / durationSeconds) или runUntilStopped=true");
        }

        return errors.Count == 0 ? profile : null;
    }
}
