namespace Writer.Configuration;

/// <summary>
/// Значения нагрузки по умолчанию; запрос старта может переопределить любое.
/// </summary>
public sealed class LoadProfileDefaults
{
    /// <summary>
    /// Цель: суммарное число сообщений (>= 1); null — цель не задана.
    /// Прогон завершается по первой достигнутой цели.
    /// </summary>
    public int? TotalMessages { get; set; }

    /// <summary>
    /// Цель: суммарный объём записи в байтах (>= 1); null — цель не задана.
    /// Считается по размеру body сообщений (на диспетчеризации — по оценке).
    /// </summary>
    public long? TotalPayloadBytes { get; set; }

    /// <summary>
    /// Цель: длительность прогона в секундах (>= 1); null — цель не задана.
    /// </summary>
    public int? DurationSeconds { get; set; }

    /// <summary>
    /// true — генерировать до явного POST /api/generation/stop (цели завершения не требуются).
    /// </summary>
    public bool RunUntilStopped { get; set; }

    /// <summary>
    /// Размер payload одного сообщения в байтах [1..10000000];
    /// итоговый body = payload + фиксированные поля конверта.
    /// </summary>
    public int MessageSizeBytes { get; set; } = 1024;

    /// <summary>
    /// null — без ограничения скорости.
    /// </summary>
    public int? RatePerSecond { get; set; }

    /// <summary>
    /// Максимум одновременно выполняемых батчей [1..64].
    /// </summary>
    public int Parallelism { get; set; } = 4;

    /// <summary>
    /// Сообщений в одном батче [1..5000]: одна транзакция INSERT+DELETE
    /// (6 параметров на строку, лимит протокола 65535).
    /// </summary>
    public int BatchSize { get; set; } = 100;
}
