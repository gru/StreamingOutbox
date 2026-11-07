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
