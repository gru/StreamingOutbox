using System.Globalization;
using System.Text;
using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Partitions;
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
public sealed class SecurityDefinerSqlFunctionsTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task ProductionInitScript_SecurityDefinerFunctionsAndPublishSwitch_WorkUnderLeastPrivilegeRoles()
    {
        var writerPassword = "it_writer_pwd_123";
        var replPassword = "it_repl_pwd_123";
        await EnsureLeastPrivilegeRolesAndSchemaAsync(writerPassword, replPassword);
        await ApplyProductionInitScriptAsync();

        var baseConnString = fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection).Value.ConnectionString;
        var replConnString = new NpgsqlConnectionStringBuilder(baseConnString)
        {
            Username = "sys_replication",
            Password = replPassword,
        }.ConnectionString;
        var writerConnString = new NpgsqlConnectionStringBuilder(baseConnString)
        {
            Username = "sys_writer",
            Password = writerPassword,
        }.ConnectionString;

        var slotName = IntegrationFixture.NewName();
        var checkpointTopic = IntegrationFixture.NewName();
        var eventsTopic = IntegrationFixture.NewName();
        var prefix = $"sd_{Guid.NewGuid():N}";
        await fixture.CreateTopicAsync(checkpointTopic, partitions: 1);
        await fixture.CreateTopicAsync(eventsTopic, partitions: 1);

        var replPostgresOptions = Options.Create(new PostgresOptions { ConnectionString = replConnString });
        var replicationOptions = fixture.BuildOptions<ReplicationOptions>(
            ReplicationOptions.ConfigurationSection,
            ("CdcOutbox:Replication:SlotName", slotName),
            ("CdcOutbox:Replication:PublicationName", "outbox_partitioned_publication"),
            ("CdcOutbox:Replication:Binary", "true"),
            ("CdcOutbox:Replication:FlushSlotOnIdle", "true"));
        var storageOptions = fixture.BuildOptions<OutboxStorageOptions>(
            OutboxStorageOptions.ConfigurationSection,
            ("CdcOutbox:Storage:Schema", "outbox"),
            ("CdcOutbox:Storage:Table", "messages_partitioned"),
            ("CdcOutbox:Storage:Mode", nameof(OutboxStorageMode.PartitionedTable)),
            ("CdcOutbox:Storage:EnableWalMessages", "true"),
            ("CdcOutbox:Storage:WalMessagePrefix", prefix));
        var partitionOptions = Options.Create(new PartitionMaintenanceOptions
        {
            Enabled = true,
            PrecreateHours = 48,
            RetentionHours = 0,
            SealGraceMinutes = 5,
            UseSecurityDefinerFunctions = true,
        });

        // 1. Проверяем SECURITY DEFINER-функции управления партициями от имени бесправной роли sys_replication
        var registryClient = new PartitionRegistryClient(
            replPostgresOptions,
            replicationOptions,
            storageOptions,
            partitionOptions,
            NullLogger<PartitionRegistryClient>.Instance);

        var pastHour = new DateTimeOffset(2026, 1, 10, 8, 5, 0, TimeSpan.Zero);
        var ensuredPast = await registryClient.EnsurePartitionsAsync(pastHour, CancellationToken.None);
        ensuredPast.FuturePartitionsCount.ShouldBeGreaterThanOrEqualTo(48);

        var nowUtc = DateTimeOffset.UtcNow;
        var ensuredNow = await registryClient.EnsurePartitionsAsync(nowUtc, CancellationToken.None);
        ensuredNow.FuturePartitionsCount.ShouldBeGreaterThanOrEqualTo(48);

        var sysId = await registryClient.GetSystemIdentifierAsync(CancellationToken.None);
        var sealTime = pastHour.AddHours(1);
        var sealedCount = await registryClient.SealClosedPartitionsAsync(sealTime, CancellationToken.None);
        sealedCount.ShouldBeGreaterThanOrEqualTo(1);

        await registryClient.EnsureGuardInitializedAsync(sysId, 0x10UL, pastHour.UtcDateTime, CancellationToken.None);
        var droppedBeforeShift = await registryClient.DropDeliveredPartitionsAsync(0x10UL, sysId, sealTime, CancellationToken.None);
        droppedBeforeShift.ShouldBeEmpty();

        // Сдвигаем LSN выше текущего WAL и проверяем безопасное удаление закрытой партиции через SECURITY DEFINER
        var currentLsnText = (await fixture.QueryAsync("SELECT pg_current_wal_lsn()::text", r => r.GetString(0))).Single();
        var currentLsn = LsnText.TryParse(currentLsnText)!.Value;
        await registryClient.UpdateDeliveryProgressAsync(currentLsn, sealTime.UtcDateTime, sysId, CancellationToken.None);

        var droppedAfterShift = await registryClient.DropDeliveredPartitionsAsync(currentLsn, sysId, sealTime, CancellationToken.None);
        droppedAfterShift.ShouldContain("messages_partitioned_20260110_08");

        // 2. Создаём failover-слот от имени sys_replication и проверяем публикацию outbox_partitioned_publication
        var provisioner = new OutboxSlotProvisioner(
            replPostgresOptions,
            replicationOptions,
            NullLogger<OutboxSlotProvisioner>.Instance);
        await provisioner.CreateFailoverSlotAndReturnLsnAsync(CancellationToken.None);

        var verifier = new PublicationVerifier(
            replPostgresOptions,
            replicationOptions,
            storageOptions,
            NullLogger<PublicationVerifier>.Instance);
        await verifier.VerifyAsync(CancellationToken.None);

        // 3. От имени бесправной роли sys_writer вызываем единую outbox.publish_message с переключателем:
        //    (a) p_write_to_table = true  → INSERT в текущую часовую партицию
        //    (b) p_write_to_table = false → прямой WAL emit в OBX1 без записи в таблицу
        //    (c) outbox.emit_frame        → пакетный SuperFrame из 2 сообщений
        long rowsBefore;
        long rowsAfterTableWrite;
        long rowsAfterWalWrite;
        await using (var writerConn = new NpgsqlConnection(writerConnString))
        {
            await writerConn.OpenAsync();

            rowsBefore = (await fixture.QueryAsync("SELECT count(*) FROM outbox.messages_partitioned", r => r.GetInt64(0))).Single();

            await using (var cmd1 = new NpgsqlCommand(
                "SELECT outbox.publish_message($1, $2, $3, '{\"mode\":\"table\"}', NULL, NULL, now(), true, $4)",
                writerConn))
            {
                cmd1.Parameters.Add(new() { Value = eventsTopic });
                cmd1.Parameters.Add(new() { Value = Encoding.UTF8.GetBytes("sw-k1") });
                cmd1.Parameters.Add(new() { Value = Encoding.UTF8.GetBytes("switch-1-table") });
                cmd1.Parameters.Add(new() { Value = prefix });
                var id1 = Convert.ToInt64(await cmd1.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
                id1.ShouldBeGreaterThan(0);
            }

            rowsAfterTableWrite = (await fixture.QueryAsync("SELECT count(*) FROM outbox.messages_partitioned", r => r.GetInt64(0))).Single();
            rowsAfterTableWrite.ShouldBe(rowsBefore + 1);

            await using (var cmd2 = new NpgsqlCommand(
                "SELECT outbox.publish_message($1, $2, $3, '{\"mode\":\"wal\"}', NULL, NULL, now(), false, $4)",
                writerConn))
            {
                cmd2.Parameters.Add(new() { Value = eventsTopic });
                cmd2.Parameters.Add(new() { Value = Encoding.UTF8.GetBytes("sw-k2") });
                cmd2.Parameters.Add(new() { Value = Encoding.UTF8.GetBytes("switch-2-wal") });
                cmd2.Parameters.Add(new() { Value = prefix });
                var id2 = Convert.ToInt64(await cmd2.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
                id2.ShouldBeGreaterThan(0);
            }

            rowsAfterWalWrite = (await fixture.QueryAsync("SELECT count(*) FROM outbox.messages_partitioned", r => r.GetInt64(0))).Single();
            rowsAfterWalWrite.ShouldBe(rowsAfterTableWrite);

            var batchFrame = WalOutboxFrameCodec.Encode(new[]
            {
                new OutboxMessage
                {
                    Id = 5001,
                    Topic = eventsTopic,
                    Key = Encoding.UTF8.GetBytes("sw-k3"),
                    Body = Encoding.UTF8.GetBytes("switch-3-frame"),
                    Headers = "{}",
                    CreatedAt = DateTime.UtcNow,
                },
                new OutboxMessage
                {
                    Id = 5002,
                    Topic = eventsTopic,
                    Key = Encoding.UTF8.GetBytes("sw-k4"),
                    Body = Encoding.UTF8.GetBytes("switch-4-frame"),
                    Headers = "{}",
                    CreatedAt = DateTime.UtcNow,
                },
            });

            await using (var cmd3 = new NpgsqlCommand("SELECT outbox.emit_frame($1, $2)", writerConn))
            {
                cmd3.Parameters.Add(new() { Value = batchFrame });
                cmd3.Parameters.Add(new() { Value = prefix });
                await cmd3.ExecuteScalarAsync();
            }
        }

        // 4. Читаем слот от имени sys_replication и убеждаемся, что все 4 сообщения доставились в Kafka в точном порядке
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName);
        var progress = new ReplicationProgress();
        var reader = new OutboxSlotReader(
            replPostgresOptions,
            replicationOptions,
            storageOptions,
            Options.Create(new ProcessorOptions { CheckpointIntervalSeconds = 1 }),
            progress,
            TimeProvider.System,
            metrics,
            NullLogger<OutboxSlotReader>.Instance);

        var kafkaOptions = fixture.BuildOptions<KafkaOptions>(
            KafkaOptions.ConfigurationSection,
            ("CdcOutbox:Kafka:CheckpointTopic", checkpointTopic));
        var checkpointStore = new CheckpointStore(
            fixture.TransactionalProducer,
            kafkaOptions,
            replicationOptions,
            TimeProvider.System,
            NullLogger<CheckpointStore>.Instance);
        var publisher = new OutboxPublisher(
            fixture.TransactionalProducer,
            Options.Create(new ProcessorOptions { BatchSize = 50, CheckpointIntervalSeconds = 1 }),
            TimeProvider.System,
            metrics,
            progress,
            checkpointStore,
            NullLogger<OutboxPublisher>.Instance,
            replicationOptions,
            storageOptions);

        var channel = Channel.CreateBounded<OutboxMessage>(128);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var readTask = reader.ReadAsync(channel.Writer, cts.Token);
        var publishTask = publisher.PublishAsync(channel.Reader, CancellationToken.None);

        var sentPrefix = $"outbox_processor_sent_messages_total{{slot=\"{slotName}\"}} ";
        var delivered = await Wait.UntilAsync(async () =>
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
                    && value >= 4)
                {
                    return true;
                }
            }

            return false;
        }, TimeSpan.FromSeconds(35));
        delivered.ShouldBeTrue();

        await cts.CancelAsync();
        await readTask;
        channel.Writer.TryComplete();
        await publishTask;

        var consumed = fixture.ConsumeTopic(eventsTopic, expected: 4, idleWindow: TimeSpan.FromSeconds(1));
        var bodies = consumed.Select(c => Encoding.UTF8.GetString(c.Message.Value)).ToArray();
        bodies.ShouldBe(new[] { "switch-1-table", "switch-2-wal", "switch-3-frame", "switch-4-frame" });
    }

    private async Task EnsureLeastPrivilegeRolesAndSchemaAsync(string writerPassword, string replPassword)
    {
        await fixture.ExecuteAsync(
            "DO $$ BEGIN " +
            $"IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_writer') THEN CREATE ROLE sys_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD '{writerPassword}'; ELSE ALTER ROLE sys_writer PASSWORD '{writerPassword}'; END IF; " +
            $"IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_replication') THEN CREATE ROLE sys_replication LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOINHERIT PASSWORD '{replPassword}'; ELSE ALTER ROLE sys_replication PASSWORD '{replPassword}'; END IF; " +
            "END $$;");

        await fixture.ExecuteAsync("CREATE SCHEMA IF NOT EXISTS outbox");
        await fixture.ExecuteAsync("GRANT CONNECT ON DATABASE outbox TO sys_writer, sys_replication");
        await fixture.ExecuteAsync("GRANT USAGE ON SCHEMA outbox TO sys_writer, sys_replication");
    }

    private async Task ApplyProductionInitScriptAsync()
    {
        var scriptPath = FindInitScriptPath();
        var lines = await File.ReadAllLinesAsync(scriptPath);

        var sqlBuffer = new StringBuilder();
        foreach (var rawLine in lines)
        {
            var trimmed = rawLine.Trim();
            if (trimmed.StartsWith(@"\connect", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (trimmed.EndsWith(@"\gexec", StringComparison.OrdinalIgnoreCase))
            {
                var selectLine = rawLine[..rawLine.LastIndexOf(@"\gexec", StringComparison.OrdinalIgnoreCase)];
                sqlBuffer.AppendLine(selectLine);
                var queryText = sqlBuffer.ToString().Trim();
                sqlBuffer.Clear();

                var statements = await fixture.QueryAsync(queryText, r => r.GetString(0));
                foreach (var statement in statements)
                {
                    if (!string.IsNullOrWhiteSpace(statement))
                    {
                        await fixture.ExecuteAsync(statement);
                    }
                }

                continue;
            }

            // Если встречаем разделитель перед SELECT ...\gexec, выполняем накопленный блок DDL
            if (trimmed.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase)
                && sqlBuffer.Length > 0
                && !sqlBuffer.ToString().Contains("$$", StringComparison.Ordinal))
            {
                await ExecuteIfNotBlankAsync(sqlBuffer.ToString());
                sqlBuffer.Clear();
            }

            sqlBuffer.AppendLine(rawLine);

            // Как только закрылось тело функции ($$;) — выполняем блок, чтобы не смешивать с последующими \gexec
            if (trimmed.Equals("$$;", StringComparison.Ordinal))
            {
                await ExecuteIfNotBlankAsync(sqlBuffer.ToString());
                sqlBuffer.Clear();
            }
        }

        await ExecuteIfNotBlankAsync(sqlBuffer.ToString());
    }

    private async Task ExecuteIfNotBlankAsync(string sql)
    {
        var cleaned = string.Join(
            '\n',
            sql.Split('\n').Where(l => !l.TrimStart().StartsWith("--", StringComparison.Ordinal))).Trim();
        if (!string.IsNullOrWhiteSpace(cleaned))
        {
            await fixture.ExecuteAsync(cleaned);
        }
    }

    private static string FindInitScriptPath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "infrastructure", "postgres", "init", "04-wal-and-partitions.sql");
            if (File.Exists(candidate))
            {
                return candidate;
            }

            dir = dir.Parent;
        }

        throw new FileNotFoundException("Не найден скрипт infrastructure/postgres/init/04-wal-and-partitions.sql от каталога тестов.");
    }
}
