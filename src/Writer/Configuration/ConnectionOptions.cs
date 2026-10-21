namespace Writer.Configuration;

/// <summary>
/// Подключение к PostgreSQL напрямую через HAProxy.
/// </summary>
public sealed class ConnectionOptions
{
    /// <summary>
    /// Хост HAProxy (сервис сети стенда или localhost).
    /// </summary>
    public string Host { get; set; } = "haproxy";

    /// <summary>
    /// TCP-порт HAProxy.
    /// </summary>
    public int Port { get; set; } = 5432;

    /// <summary>
    /// Прикладная БД и учётные данные роли записи.
    /// </summary>
    public DatabaseOptions Database { get; set; } = new();
}
