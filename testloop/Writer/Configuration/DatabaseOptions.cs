namespace Writer.Configuration;

/// <summary>
/// Прикладная БД и роль записи Writer.
/// </summary>
public sealed class DatabaseOptions
{
    /// <summary>
    /// Прикладная БД (outbox-таблица и слот живут в ней).
    /// </summary>
    public string Name { get; set; } = "outbox";

    /// <summary>
    /// Роль записи: INSERT+DELETE в outbox.messages (гранты — dependencies.md §3).
    /// </summary>
    public string Username { get; set; } = "sys_writer";

    /// <summary>
    /// Пароль роли; передаётся через env (WRITER__CONNECTION__DATABASE__PASSWORD), не appsettings.
    /// </summary>
    public string Password { get; set; } = "";
}
