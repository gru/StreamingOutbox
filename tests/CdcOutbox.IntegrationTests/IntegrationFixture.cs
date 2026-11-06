using CdcOutbox.Core.Configuration;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
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
        Producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = bootstrapServers })
            .Build();
        // конвейер публикует только транзакциями (Begin/Commit), обычный Producer
        // не подошёл; transactional.id уникален на прогон, чтобы не блокировать
        // продюсеров других прогонов
        TransactionalProducer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            TransactionalId = $"it-transactional-{Guid.NewGuid():N}",
        }).Build();
        TransactionalProducer.InitTransactions(TimeSpan.FromSeconds(30));
    }

    public IAdminClient AdminClient { get; }

    public IProducer<byte[], byte[]> Producer { get; }

    public IProducer<byte[], byte[]> TransactionalProducer { get; }

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

    public async Task InsertOutboxMessagesAsync(string schema, params OutboxInsert[] messages)
    {
        var values = new List<string>();
        var arguments = new List<object?>();
        foreach (var message in messages)
        {
            var parameterBase = arguments.Count;
            values.Add(
                $"(${parameterBase + 1}, ${parameterBase + 2}, ${parameterBase + 3}, " +
                $"${parameterBase + 4}, ${parameterBase + 5}, ${parameterBase + 6}, ${parameterBase + 7})");
            arguments.Add(message.Topic);
            arguments.Add(message.Key);
            arguments.Add(message.Body);
            arguments.Add(message.Headers);
            arguments.Add(message.Partition);
            arguments.Add(message.ProducerKey);
            arguments.Add(message.CreatedAt.HasValue
                ? DateTime.SpecifyKind(message.CreatedAt.Value, DateTimeKind.Utc)
                : null);
        }

        // INSERT и DELETE — одной транзакцией, как Writer: сообщение живёт в WAL,
        // таблица всегда пустая
        await using var connection = await OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var command = CreateCommand(
            $"INSERT INTO {schema}.messages (topic, key, body, headers, partition, producer_key, created_at) VALUES {string.Join(", ", values)}",
            connection,
            arguments.ToArray()))
        {
            await command.ExecuteNonQueryAsync();
        }

        await using (var command = new NpgsqlCommand($"DELETE FROM {schema}.messages", connection, transaction))
        {
            await command.ExecuteNonQueryAsync();
        }

        await transaction.CommitAsync();
    }

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

    public async Task CreateTopicAsync(string topicName, int partitions = 1)
    {
        await AdminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification { Name = topicName, NumPartitions = partitions, ReplicationFactor = 1 },
        });
        for (var attempt = 0; attempt < 60; attempt++)
        {
            var metadata = AdminClient.GetMetadata(topicName, TimeSpan.FromSeconds(5));
            var topic = metadata.Topics.SingleOrDefault(candidate => candidate.Topic == topicName);
            if (topic is not null && topic.Partitions.All(partition => partition.Leader >= 0))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }

        throw new TimeoutException($"Топик '{topicName}' не получил лидеров партиций за 30 с после создания.");
    }

    public List<ConsumeResult<byte[], byte[]>> ConsumeTopic(string topic, int expected, TimeSpan? idleWindow = null)
    {
        var bootstrapServers = BuildOptions<KafkaOptions>(KafkaOptions.ConfigurationSection)
            .Value.BootstrapServers;
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = NewName(),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            // события публикуются транзакциями: читаем только зафиксированное
            IsolationLevel = IsolationLevel.ReadCommitted,
        }).Build();
        consumer.Subscribe(topic);

        var results = new List<ConsumeResult<byte[], byte[]>>();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (results.Count < expected && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(2));
            if (result is not null)
            {
                results.Add(result);
            }
        }

        var idleUntil = DateTime.UtcNow + (idleWindow ?? TimeSpan.Zero);
        while (DateTime.UtcNow < idleUntil)
        {
            var extra = consumer.Consume(TimeSpan.FromSeconds(1));
            if (extra is not null)
            {
                results.Add(extra);
            }
        }

        return results;
    }

    public void Dispose()
    {
        Producer.Dispose();
        TransactionalProducer.Dispose();
        AdminClient.Dispose();
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync()
    {
        var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(CancellationToken.None);
        return connection;
    }

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
