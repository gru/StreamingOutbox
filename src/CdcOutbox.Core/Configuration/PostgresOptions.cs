using System.ComponentModel.DataAnnotations;
using Npgsql;

namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Подключение к PostgreSQL.
/// </summary>
public sealed class PostgresOptions
{
    /// <summary>
    /// Путь секции конфигурации.
    /// </summary>
    public const string ConfigurationSection = "CdcOutbox:Postgres";

    /// <summary>
    /// Строка подключения Npgsql.
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Postgres:ConnectionString не задана.")]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Проверить корректность строки подключения.
    /// </summary>
    /// <param name="connectionString">Строка подключения.</param>
    /// <returns>Описание проблемы; null, если строка корректна.</returns>
    public static string? FindConnectionIssue(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return "CdcOutbox:Postgres:ConnectionString не задана.";
        }

        NpgsqlConnectionStringBuilder builder;
        try
        {
            builder = new NpgsqlConnectionStringBuilder(connectionString);
        }
        catch (ArgumentException ex)
        {
            return $"CdcOutbox:Postgres:ConnectionString не разбирается Npgsql: {ex.Message}";
        }

        if (string.IsNullOrWhiteSpace(builder.Host))
        {
            return "CdcOutbox:Postgres:ConnectionString: отсутствует Host.";
        }

        if (string.IsNullOrWhiteSpace(builder.Username))
        {
            return "CdcOutbox:Postgres:ConnectionString: отсутствует Username.";
        }

        if (string.IsNullOrWhiteSpace(builder.Database))
        {
            return "CdcOutbox:Postgres:ConnectionString: отсутствует Database.";
        }

        return null;
    }
}
