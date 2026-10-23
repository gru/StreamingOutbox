using CdcOutbox.Core.Configuration;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CdcOutbox.IntegrationTests;

public sealed class IntegrationFixture : IDisposable
{
    private readonly IConfiguration _environmentConfiguration = new ConfigurationBuilder()
        .AddEnvironmentVariables()
        .Build();

    private readonly string _connectionString;

    public IntegrationFixture()
    {
        _connectionString = BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection)
            .Value.ConnectionString;
        var bootstrapServers = BuildOptions<KafkaOptions>(KafkaOptions.ConfigurationSection)
            .Value.BootstrapServers;
        AdminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers })
            .Build();
    }

    public IAdminClient AdminClient { get; }

    public IOptions<TOptions> BuildOptions<TOptions>(string section, params (string Key, string? Value)[] overrides)
        where TOptions : class
    {
        var configuration = new ConfigurationBuilder()
            .AddConfiguration(_environmentConfiguration)
            .AddInMemoryCollection(overrides.Select(pair => KeyValuePair.Create(pair.Key, pair.Value)))
            .Build();

        var services = new ServiceCollection();
        services.AddOptions<TOptions>()
            .Bind(configuration.GetSection(section))
            .ValidateDataAnnotations();

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<TOptions>>();
        _ = options.Value;
        return options;
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync()
    {
        var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(CancellationToken.None);
        return connection;
    }

    public async Task ExecuteAsync(string sql, params object?[] values)
    {
        await using var connection = await OpenConnectionAsync();
        await using var command = CreateCommand(sql, connection, values);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IReadOnlyList<T>> QueryAsync<T>(string sql, Func<NpgsqlDataReader, T> map, params object?[] values)
    {
        await using var connection = await OpenConnectionAsync();
        await using var command = CreateCommand(sql, connection, values);
        await using var reader = await command.ExecuteReaderAsync();
        var results = new List<T>();
        while (await reader.ReadAsync())
        {
            results.Add(map(reader));
        }

        return results;
    }

    public Task<IReadOnlyList<SlotState>> GetSlotsAsync(string slotName) =>
        QueryAsync(
            "SELECT slot_name, plugin, failover, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            reader => new SlotState(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetBoolean(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)),
            slotName);

    public static string NewName() => $"it_{Guid.NewGuid():N}";

    public static string NewSchemaName() => $"it_s_{Guid.NewGuid():N}";

    public Task CreateSchemaAsync(string schema) => ExecuteAsync($"CREATE SCHEMA {schema}");

    public Task CreateOutboxMessagesTableAsync(string schema) =>
        ExecuteAsync(
            "CREATE TABLE " + schema + ".messages (" +
            "id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
            "topic varchar(255) NOT NULL, " +
            "key bytea NOT NULL, " +
            "body bytea NOT NULL, " +
            "headers varchar NOT NULL, " +
            "partition integer, " +
            "producer_key varchar(255), " +
            "created_at timestamptz NOT NULL DEFAULT now(), " +
            "outbox_partition integer NOT NULL DEFAULT 0)");

    public Task CreateExtraTableAsync(string schema, string table) =>
        ExecuteAsync($"CREATE TABLE {schema}.{table} (id integer PRIMARY KEY, body text)");

    public Task CreatePublicationAsync(string publicationName, string schema, IReadOnlyCollection<string> tables, string publish = "insert") =>
        ExecuteAsync(
            $"CREATE PUBLICATION {publicationName} FOR TABLE {string.Join(", ", tables.Select(table => $"{schema}.{table}"))} WITH (publish = '{publish}')");

    public Task CreateLogicalSlotAsync(string slotName, string plugin = "pgoutput", bool? failover = null) =>
        failover is null
            ? ExecuteAsync("SELECT lsn FROM pg_create_logical_replication_slot($1, $2)", slotName, plugin)
            : ExecuteAsync(
                "SELECT lsn FROM pg_create_logical_replication_slot(slot_name => $1, plugin => $2, failover => $3)",
                slotName,
                plugin,
                failover);

    public void Dispose() => AdminClient.Dispose();

    private static NpgsqlCommand CreateCommand(string sql, NpgsqlConnection connection, object?[] values)
    {
        var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
        {
            command.Parameters.Add(new() { Value = value ?? DBNull.Value });
        }

        return command;
    }
}
