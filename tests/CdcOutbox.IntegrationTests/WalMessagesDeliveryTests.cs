using System.Globalization;
using System.Text;
using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Prometheus;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

[Collection("integration")]
public sealed class WalMessagesDeliveryTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task WalMessages_SinglePlPgSqlAndSuperFrame_DeliveredOverEmptyPublicationWithoutTable()
    {
        var schema = IntegrationFixture.NewSchemaName();
        var publicationName = IntegrationFixture.NewName();
        var slotName = IntegrationFixture.NewName();
        var checkpointTopic = IntegrationFixture.NewName();
        var eventsTopic = IntegrationFixture.NewName();
        var prefix = $"obx_{Guid.NewGuid():N}";

        await fixture.CreateSchemaAsync(schema);
        await fixture.ExecuteAsync($"CREATE PUBLICATION {publicationName} WITH (publish = 'insert')");
        await fixture.CreateLogicalSlotAsync(slotName, "pgoutput", failover: true);
        await fixture.CreateTopicAsync(checkpointTopic, partitions: 1);
        await fixture.CreateTopicAsync(eventsTopic, partitions: 3);

        // Проверяем, что PublicationVerifier принимает пустую publication в режиме WalMessages
        var verifier = new PublicationVerifier(
            fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection),
            fixture.BuildOptions<ReplicationOptions>(
                ReplicationOptions.ConfigurationSection,
                ("CdcOutbox:Replication:SlotName", slotName),
                ("CdcOutbox:Replication:PublicationName", publicationName)),
            fixture.BuildOptions<OutboxStorageOptions>(
                OutboxStorageOptions.ConfigurationSection,
                ("CdcOutbox:Storage:Schema", schema),
                ("CdcOutbox:Storage:Table", "unused_table"),
                ("CdcOutbox:Storage:Mode", nameof(OutboxStorageMode.WalMessages)),
                ("CdcOutbox:Storage:WalMessagePrefix", prefix)),
            NullLogger<PublicationVerifier>.Instance);
        await verifier.VerifyAsync(CancellationToken.None);

        // 1. Одиночное сообщение, упакованное в OBX1 на стороне PostgreSQL (int4send/int8send/set_byte)
        var sqlKey = Encoding.UTF8.GetBytes("sql-key-1");
        var sqlBody = Encoding.UTF8.GetBytes("{\"origin\":\"plpgsql\",\"value\":42}");
        await fixture.ExecuteAsync(
            "SELECT pg_logical_emit_message(true, $4, " +
            "'\\x4f425831'::bytea || int4send(1) || int8send(1001) " +
            "|| int8send(trunc(extract(epoch FROM now()) * 1000000)::bigint) " +
            "|| set_byte('\\x00'::bytea, 0, 3) || int4send(2) " +
            "|| int4send(octet_length(convert_to($1, 'UTF8'))) || convert_to($1, 'UTF8') " +
            "|| int4send(octet_length($2)) || $2 " +
            "|| int4send(octet_length($3)) || $3 " +
            "|| int4send(octet_length(convert_to('{\"x-src\":\"pg\"}', 'UTF8'))) || convert_to('{\"x-src\":\"pg\"}', 'UTF8') " +
            "|| int4send(octet_length(convert_to('cluster-a', 'UTF8'))) || convert_to('cluster-a', 'UTF8'))",
            eventsTopic,
            sqlKey,
            sqlBody,
            prefix);

        // 2. Откаченная транзакция — не должна попасть в Kafka
        var rolledBackFrame = WalOutboxFrameCodec.Encode(new[]
        {
            new OutboxMessage
            {
                Id = 9999,
                Topic = eventsTopic,
                Key = Encoding.UTF8.GetBytes("rolled-back"),
                Body = Encoding.UTF8.GetBytes("{\"rolledBack\":true}"),
                Headers = "{}",
                CreatedAt = DateTime.UtcNow,
            },
        });
        var connectionString = fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection).Value.ConnectionString;
        await using (var conn = new NpgsqlConnection(connectionString))
        {
            await conn.OpenAsync();
            await using var tx = await conn.BeginTransactionAsync();
            await using var rollbackCmd = new NpgsqlCommand("SELECT pg_logical_emit_message(true, $1, $2)", conn, tx);
            rollbackCmd.Parameters.Add(new() { Value = prefix });
            rollbackCmd.Parameters.Add(new() { Value = rolledBackFrame });
            await rollbackCmd.ExecuteNonQueryAsync();
            await tx.RollbackAsync();
        }

        // 3. Сообщение с чужим префиксом — должно быть проигнорировано без сбоя протокола
        await fixture.ExecuteAsync(
            "SELECT pg_logical_emit_message(true, 'foreign_prefix', '\\xdeadbeef'::bytea)");

        // 4. Пакетный SuperFrame из 5 сообщений (WalOutboxFrameCodec.Encode)
        var batchMessages = Enumerable.Range(1, 5).Select(i => new OutboxMessage
        {
            Id = 2000 + i,
            Topic = eventsTopic,
            Key = Encoding.UTF8.GetBytes($"batch-key-{i}"),
            Body = Encoding.UTF8.GetBytes($"{{\"seq\":{i}}}"),
            Headers = $"{{\"n\":\"{i}\"}}",
            Partition = i % 3,
            CreatedAt = DateTime.UtcNow,
        }).ToArray();
        var superFrame = WalOutboxFrameCodec.Encode(batchMessages);
        await fixture.ExecuteAsync(
            "SELECT pg_logical_emit_message(true, $1, $2)",
            prefix,
            superFrame);

        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName);
        var progress = new ReplicationProgress();
        var reader = CreateSlotReader(schema, "unused_table", publicationName, slotName, OutboxStorageMode.WalMessages, enableWalMessages: true, prefix, progress, metrics);
        var publisher = CreatePublisher(slotName, checkpointTopic, progress, metrics);

        await RunPipelineUntilDeliveredAsync(reader, publisher, progress, registry, slotName, expectedCount: 6);

        var consumed = fixture.ConsumeTopic(eventsTopic, expected: 6, idleWindow: TimeSpan.FromSeconds(2));
        consumed.Count.ShouldBe(6);

        var bodies = consumed.Select(c => Encoding.UTF8.GetString(c.Message.Value)).ToHashSet();
        bodies.ShouldContain("{\"origin\":\"plpgsql\",\"value\":42}");
        bodies.ShouldNotContain("{\"rolledBack\":true}");
        for (var i = 1; i <= 5; i++)
        {
            bodies.ShouldContain($"{{\"seq\":{i}}}");
        }

        // Проверяем, что PL/pgSQL-сообщение попало в заданную партицию 2 и сохранило заголовок
        var sqlRecord = consumed.Single(c => Encoding.UTF8.GetString(c.Message.Key) == "sql-key-1");
        sqlRecord.Partition.Value.ShouldBe(2);
        Encoding.UTF8.GetString(sqlRecord.Message.Headers.GetLastBytes("x-src")).ShouldBe("pg");

        var metricsText = await MetricsExport.ExportAsync(registry);
        metricsText.ShouldContain($"outbox_wal_messages_decoded_total{{slot=\"{slotName}\"}} 6");
    }

    [Fact]
    public async Task HotToggle_InsertAndWalEmitOnSameSlot_DeliversBothInOrderWithoutRestart()
    {
        var schema = IntegrationFixture.NewSchemaName();
        var publicationName = IntegrationFixture.NewName();
        var slotName = IntegrationFixture.NewName();
        var checkpointTopic = IntegrationFixture.NewName();
        var eventsTopic = IntegrationFixture.NewName();
        var prefix = $"mix_{Guid.NewGuid():N}";

        await fixture.CreateSchemaAsync(schema);
        await fixture.ExecuteAsync(
            $"CREATE TABLE {schema}.messages (" +
            "id bigint GENERATED ALWAYS AS IDENTITY NOT NULL, " +
            "topic varchar(255) NOT NULL, " +
            "key bytea NOT NULL, " +
            "body bytea NOT NULL, " +
            "headers varchar NOT NULL, " +
            "partition integer, " +
            "producer_key varchar(255), " +
            "created_at timestamptz NOT NULL DEFAULT now(), " +
            "outbox_partition integer NOT NULL DEFAULT 0, " +
            "PRIMARY KEY (created_at, id)) PARTITION BY RANGE (created_at)");

        var hourStart = new DateTimeOffset(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, 0, 0, TimeSpan.Zero);
        await fixture.ExecuteAsync(
            $"CREATE TABLE {schema}.messages_cur PARTITION OF {schema}.messages " +
            $"FOR VALUES FROM ('{hourStart:yyyy-MM-dd HH:mm:ss+00}') TO ('{hourStart.AddHours(2):yyyy-MM-dd HH:mm:ss+00}')");

        await fixture.ExecuteAsync(
            $"CREATE PUBLICATION {publicationName} FOR TABLE {schema}.messages WITH (publish = 'insert', publish_via_partition_root = true)");
        await fixture.CreateLogicalSlotAsync(slotName, "pgoutput", failover: true);
        await fixture.CreateTopicAsync(checkpointTopic, partitions: 1);
        await fixture.CreateTopicAsync(eventsTopic, partitions: 1);

        // 1. Пишем 2 сообщения обычным INSERT (режим с таблицей)
        await fixture.ExecuteAsync(
            $"INSERT INTO {schema}.messages (topic, key, body, headers, created_at) VALUES " +
            "($1, $2, $3, '{}', now()), ($1, $4, $5, '{}', now())",
            eventsTopic,
            Encoding.UTF8.GetBytes("k1"),
            Encoding.UTF8.GetBytes("msg-1-table"),
            Encoding.UTF8.GetBytes("k2"),
            Encoding.UTF8.GetBytes("msg-2-table"));

        // 2. Переключаемся на лету на WAL emit без пересоздания слота и пишем ещё 2 сообщения
        var walFrame = WalOutboxFrameCodec.Encode(new[]
        {
            new OutboxMessage
            {
                Id = 3,
                Topic = eventsTopic,
                Key = Encoding.UTF8.GetBytes("k3"),
                Body = Encoding.UTF8.GetBytes("msg-3-wal"),
                Headers = "{}",
                CreatedAt = DateTime.UtcNow,
            },
            new OutboxMessage
            {
                Id = 4,
                Topic = eventsTopic,
                Key = Encoding.UTF8.GetBytes("k4"),
                Body = Encoding.UTF8.GetBytes("msg-4-wal"),
                Headers = "{}",
                CreatedAt = DateTime.UtcNow,
            },
        });
        await fixture.ExecuteAsync("SELECT pg_logical_emit_message(true, $1, $2)", prefix, walFrame);

        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName);
        var progress = new ReplicationProgress();
        var reader = CreateSlotReader(
            schema,
            "messages",
            publicationName,
            slotName,
            OutboxStorageMode.PartitionedTable,
            enableWalMessages: true,
            prefix,
            progress,
            metrics);
        var publisher = CreatePublisher(slotName, checkpointTopic, progress, metrics);

        await RunPipelineUntilDeliveredAsync(reader, publisher, progress, registry, slotName, expectedCount: 4);

        var consumed = fixture.ConsumeTopic(eventsTopic, expected: 4);
        var sequence = consumed.Select(c => Encoding.UTF8.GetString(c.Message.Value)).ToArray();
        sequence.ShouldBe(new[] { "msg-1-table", "msg-2-table", "msg-3-wal", "msg-4-wal" });
    }

    private OutboxSlotReader CreateSlotReader(
        string schema,
        string table,
        string publicationName,
        string slotName,
        OutboxStorageMode mode,
        bool enableWalMessages,
        string prefix,
        ReplicationProgress progress,
        OutboxMetrics metrics) =>
        new(
            fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection),
            fixture.BuildOptions<ReplicationOptions>(
                ReplicationOptions.ConfigurationSection,
                ("CdcOutbox:Replication:SlotName", slotName),
                ("CdcOutbox:Replication:PublicationName", publicationName),
                ("CdcOutbox:Replication:Binary", "true"),
                ("CdcOutbox:Replication:FlushSlotOnIdle", "true")),
            fixture.BuildOptions<OutboxStorageOptions>(
                OutboxStorageOptions.ConfigurationSection,
                ("CdcOutbox:Storage:Schema", schema),
                ("CdcOutbox:Storage:Table", table),
                ("CdcOutbox:Storage:Mode", mode.ToString()),
                ("CdcOutbox:Storage:EnableWalMessages", enableWalMessages.ToString()),
                ("CdcOutbox:Storage:WalMessagePrefix", prefix)),
            Options.Create(new ProcessorOptions { CheckpointIntervalSeconds = 1 }),
            progress,
            TimeProvider.System,
            metrics,
            NullLogger<OutboxSlotReader>.Instance);

    private OutboxPublisher CreatePublisher(
        string slotName,
        string checkpointTopic,
        ReplicationProgress progress,
        OutboxMetrics metrics)
    {
        var kafkaOptions = fixture.BuildOptions<KafkaOptions>(
            KafkaOptions.ConfigurationSection,
            ("CdcOutbox:Kafka:CheckpointTopic", checkpointTopic));
        var replicationOptions = fixture.BuildOptions<ReplicationOptions>(
            ReplicationOptions.ConfigurationSection,
            ("CdcOutbox:Replication:SlotName", slotName),
            ("CdcOutbox:Replication:PublicationName", "unused"));
        var checkpointStore = new CheckpointStore(
            fixture.TransactionalProducer,
            kafkaOptions,
            replicationOptions,
            TimeProvider.System,
            NullLogger<CheckpointStore>.Instance);

        return new OutboxPublisher(
            fixture.TransactionalProducer,
            Options.Create(new ProcessorOptions { BatchSize = 50, CheckpointIntervalSeconds = 1 }),
            TimeProvider.System,
            metrics,
            progress,
            checkpointStore,
            NullLogger<OutboxPublisher>.Instance,
            replicationOptions);
    }

    private static async Task RunPipelineUntilDeliveredAsync(
        OutboxSlotReader reader,
        OutboxPublisher publisher,
        ReplicationProgress progress,
        CollectorRegistry registry,
        string slotName,
        int expectedCount)
    {
        var channel = Channel.CreateBounded<OutboxMessage>(128);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var readTask = reader.ReadAsync(channel.Writer, cts.Token);
        var publishTask = publisher.PublishAsync(channel.Reader, CancellationToken.None);

        var sentPrefix = $"outbox_processor_sent_messages_total{{slot=\"{slotName}\"}} ";
        var ok = await Wait.UntilAsync(async () =>
        {
            if (progress.CheckpointedPosition == 0)
            {
                return false;
            }

            var exported = await MetricsExport.ExportAsync(registry);
            foreach (var line in exported.Split('\n'))
            {
                if (line.StartsWith(sentPrefix, StringComparison.Ordinal)
                    && double.TryParse(line[sentPrefix.Length..].Trim(), CultureInfo.InvariantCulture, out var value)
                    && value >= expectedCount)
                {
                    return true;
                }
            }

            return false;
        }, TimeSpan.FromSeconds(35));
        ok.ShouldBeTrue("сообщения не доставились в Kafka за отведённый таймаут");

        await cts.CancelAsync();
        await readTask;
        channel.Writer.TryComplete();
        await publishTask;
    }
}
