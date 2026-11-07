namespace Writer.Configuration;

/// <summary>
/// Конфигурация Writer (секция "Writer", env-префикс WRITER__).
/// </summary>
public sealed class WriterOptions
{
    /// <summary>
    /// Имя секции конфигурации в appsettings / env (WRITER__...).
    /// </summary>
    public const string SectionName = "Writer";

    /// <summary>
    /// Подключение к PostgreSQL напрямую через HAProxy.
    /// </summary>
    public ConnectionOptions Connection { get; set; } = new();

    /// <summary>
    /// Профиль нагрузки по умолчанию; любое поле переопределяется запросом старта.
    /// </summary>
    public LoadProfileDefaults Defaults { get; set; } = new();
}
