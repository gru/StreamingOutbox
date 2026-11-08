namespace Writer.Configuration;

/// <summary>
/// Прикладная БД и роль записи Writer.
/// </summary>
public sealed class DatabaseOptions
{
    /// <summary>
    /// Прикладная БД.
    /// </summary>
    public string Name { get; set; } = "outbox";

    /// <summary>
    /// Роль записи: INSERT+DELETE в outbox.messages.
    /// </summary>
    public string Username { get; set; } = "sys_writer";

    /// <summary>
    /// Пароль роли;
    /// </summary>
    public string Password { get; set; } = "";
}
