using CdcOutbox.Core.Configuration;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit;

namespace CdcOutbox.IntegrationTests;

public sealed record SlotState(string SlotName, string? Plugin, bool? Failover);

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
            "SELECT slot_name, plugin, failover FROM pg_replication_slots WHERE slot_name = $1",
            reader => new SlotState(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetBoolean(2)),
            slotName);

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

[CollectionDefinition("integration")]
public sealed class IntegrationCollection : ICollectionFixture<IntegrationFixture>
{
}
