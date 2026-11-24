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
using Prometheus;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

[Collection("integration")]
public sealed class PartitionedOutboxLifecycleTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task HourlyPartitions_Precreates48Hours_RepairsMissing_AndDropsOnlyAfterSafeLsnShift()
    {
        var schema = IntegrationFixture.NewSchemaName();
        var slotName = IntegrationFixture.NewName();
        await CreatePartitionedTableAsync(schema, "messages");

        var postgresOptions = fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection);
        var replicationOptions = fixture.BuildOptions<ReplicationOptions>(
            ReplicationOptions.ConfigurationSection,
            ("CdcOutbox:Replication:SlotName", slotName),
            ("CdcOutbox:Replication:PublicationName", "unused"));
        var storageOptions = fixture.BuildOptions<OutboxStorageOptions>(
            OutboxStorageOptions.ConfigurationSection,
            ("CdcOutbox:Storage:Schema", schema),
            ("CdcOutbox:Storage:Table", "messages"),
            ("CdcOutbox:Storage:Mode", nameof(OutboxStorageMode.PartitionedTable)));
        var partitionOptions = Options.Create(new PartitionMaintenanceOptions
        {
            Enabled = true,
            PrecreateHours = 48,
            RetentionHours = 0,
            SealGraceMinutes = 5,
            UseSecurityDefinerFunctions = false,
        });

        var client = new PartitionRegistryClient(
            postgresOptions,
            replicationOptions,
            storageOptions,
            partitionOptions,
            NullLogger<PartitionRegistryClient>.Instance);

        var t0 = new DateTimeOffset(2026, 11, 24, 12, 5, 0, TimeSpan.Zero);

        // 1. Первый запуск создаёт текущий час + 48 будущих часов = 49 секций
        var first = await client.EnsurePartitionsAsync(t0, CancellationToken.None);
        first.CreatedCount.ShouldBe(49);
        first.FuturePartitionsCount.ShouldBe(48);

        // Повторный запуск идемпотентен
        var second = await client.EnsurePartitionsAsync(t0, CancellationToken.None);
        second.CreatedCount.ShouldBe(0);
        second.FuturePartitionsCount.ShouldBe(48);

        // 2. Имитируем нехватку одной будущей партиции — проверяем, что досоздастся ровно она
        var missingPart = HourlyPartitionName.FormatPartitionName("messages", t0.AddHours(15));
        await fixture.ExecuteAsync($"DROP TABLE {schema}.{missingPart}");
        var repaired = await client.EnsurePartitionsAsync(t0, CancellationToken.None);
        repaired.CreatedCount.ShouldBe(1);
        repaired.FuturePartitionsCount.ShouldBe(48);

        // 3. Пишем строку в партицию t0 (12:00..13:00), переходим в 13:05 и запечатываем час 12:00..13:00
        await fixture.ExecuteAsync(
            $"INSERT INTO {schema}.messages (topic, key, body, headers, created_at) VALUES ('t', '\\x01', '\\x02', '{{}}', $1)",
            t0.AddMinutes(10));

        var systemId = await client.GetSystemIdentifierAsync(CancellationToken.None);
        var lsnBeforeSeal = await ReadCurrentWalLsnAsync();
        await client.EnsureGuardInitializedAsync(systemId, lsnBeforeSeal, t0.UtcDateTime, CancellationToken.None);

        var t1 = t0.AddHours(1); // 13:05:00Z — прошло ровно 5 минут после закрытия часа 12:00..13:00
        var sealedCount = await client.SealClosedPartitionsAsync(t1, CancellationToken.None);
        sealedCount.ShouldBe(1);

        // 4. Сдвига LSN после запечатывания ещё НЕ было — партиция 12:00 удалиться НЕ ДОЛЖНА!
        var droppedBeforeAdvance = await client.DropDeliveredPartitionsAsync(lsnBeforeSeal, systemId, t1, CancellationToken.None);
        droppedBeforeAdvance.ShouldBeEmpty();

        // 5. Имитируем блокировку recovery_in_progress=true даже при высоком LSN — удалять запрещено!
        await fixture.ExecuteAsync("SELECT pg_logical_emit_message(true, 'bump', '\\x01'::bytea)");
        var lsnAfterSeal = await ReadCurrentWalLsnAsync();
        await client.SetRecoveryLockAsync(true, systemId, null, null, CancellationToken.None);
        var droppedWhileLocked = await client.DropDeliveredPartitionsAsync(lsnAfterSeal, systemId, t1, CancellationToken.None);
        droppedWhileLocked.ShouldBeEmpty();

        // 6. Снимаем замок с подтверждённым LSN > sealed_lsn — теперь партиция 12:00 удаляется через DETACH CONCURRENTLY + DROP
        await client.SetRecoveryLockAsync(false, systemId, lsnAfterSeal, t1.UtcDateTime, CancellationToken.None);
        var droppedAfterAdvance = await client.DropDeliveredPartitionsAsync(lsnAfterSeal, systemId, t1, CancellationToken.None);
        droppedAfterAdvance.ShouldHaveSingleItem().ShouldBe("messages_20261124_12");

        var stillExists = await fixture.QueryAsync(
            "SELECT count(*)::integer FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2",
            r => r.GetInt32(0),
            schema,
            "messages_20261124_12");
        stillExists.Single().ShouldBe(0);
    }

    [Fact]
    public async Task SlotDroppedWhileAppInserts_AutoRecover24x7_DeliversZeroLossAndRecreatesFailoverSlot()
    {
        var schema = IntegrationFixture.NewSchemaName();
        var publicationName = IntegrationFixture.NewName();
        var slotName = IntegrationFixture.NewName();
        var checkpointTopic = IntegrationFixture.NewName();
        var eventsTopic = IntegrationFixture.NewName();

        await CreatePartitionedTableAsync(schema, "messages");
        await fixture.ExecuteAsync(
            $"CREATE PUBLICATION {publicationName} FOR TABLE {schema}.messages WITH (publish = 'insert', publish_via_partition_root = true)");
        await fixture.CreateTopicAsync(checkpointTopic, partitions: 1);
        await fixture.CreateTopicAsync(eventsTopic, partitions: 1);

        var postgresOptions = fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection);
        var replicationOptions = fixture.BuildOptions<ReplicationOptions>(
            ReplicationOptions.ConfigurationSection,
            ("CdcOutbox:Replication:SlotName", slotName),
            ("CdcOutbox:Replication:PublicationName", publicationName),
            ("CdcOutbox:Replication:Binary", "true"));
        var storageOptions = fixture.BuildOptions<OutboxStorageOptions>(
            OutboxStorageOptions.ConfigurationSection,
            ("CdcOutbox:Storage:Schema", schema),
            ("CdcOutbox:Storage:Table", "messages"),
            ("CdcOutbox:Storage:Mode", nameof(OutboxStorageMode.PartitionedTable)));
        var partitionOptions = Options.Create(new PartitionMaintenanceOptions
        {
            Enabled = true,
            PrecreateHours = 48,
            RetentionHours = 0,
            AutoRecoverFromPartitions = true,
            RecoveryBatchSize = 2,
            RecoveryOverlapMinutes = 5,
            UseSecurityDefinerFunctions = false,
        });
        var kafkaOptions = fixture.BuildOptions<KafkaOptions>(
            KafkaOptions.ConfigurationSection,
            ("CdcOutbox:Kafka:CheckpointTopic", checkpointTopic));

        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName);
        var progress = new ReplicationProgress();

        var slotProvisioner = new OutboxSlotProvisioner(postgresOptions, replicationOptions, NullLogger<OutboxSlotProvisioner>.Instance);
        var pubVerifier = new PublicationVerifier(postgresOptions, replicationOptions, storageOptions, NullLogger<PublicationVerifier>.Instance);
        var topicVerifier = new CheckpointTopicVerifier(fixture.AdminClient, kafkaOptions, NullLogger<CheckpointTopicVerifier>.Instance);
        var checkpointStore = new CheckpointStore(
            fixture.TransactionalProducer,
            kafkaOptions,
            replicationOptions,
            TimeProvider.System,
            NullLogger<CheckpointStore>.Instance);
        var registryClient = new PartitionRegistryClient(
            postgresOptions,
            replicationOptions,
            storageOptions,
            partitionOptions,
            NullLogger<PartitionRegistryClient>.Instance);
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
        var recoveryService = new PartitionRecoveryService(
            postgresOptions,
            storageOptions,
            partitionOptions,
            slotProvisioner,
            registryClient,
            publisher,
            checkpointStore,
            progress,
            TimeProvider.System,
            metrics,
            NullLogger<PartitionRecoveryService>.Instance);

        var startupChecks = new OutboxStartupChecks(
            slotProvisioner,
            pubVerifier,
            topicVerifier,
            checkpointStore,
            metrics,
            Options.Create(new CdcOutboxOptions { RecoveryMode = false }),
            NullLogger<OutboxStartupChecks>.Instance,
            storageOptions,
            partitionOptions,
            recoveryService,
            registryClient,
            progress,
            TimeProvider.System);

        // 1. Штатный первый старт: создаются 48ч партиций и failover-слот
        await startupChecks.RunAllAsync(CancellationToken.None);

        // 2. Пишем пачку 1 (2 сообщения, INSERT-only без DELETE) и доставляем в Kafka
        await InsertPartitionedRowAsync(schema, eventsTopic, "b1-1", "batch1-msg1");
        await InsertPartitionedRowAsync(schema, eventsTopic, "b1-2", "batch1-msg2");

        var reader1 = new OutboxSlotReader(
            postgresOptions, replicationOptions, storageOptions,
            Options.Create(new ProcessorOptions { CheckpointIntervalSeconds = 1 }),
            progress, TimeProvider.System, metrics, NullLogger<OutboxSlotReader>.Instance);
        await RunUntilDeliveredAndCheckpointedAsync(
            reader1, publisher, progress, registry, slotName, expectedSentTotal: 2, minCheckpointLsn: 0UL);

        var sysId = await registryClient.GetSystemIdentifierAsync(CancellationToken.None);
        await registryClient.UpdateDeliveryProgressAsync(
            progress.CheckpointedPosition, progress.LastDeliveredCreatedAtUtc, sysId, CancellationToken.None);

        // 3. АВАРИЯ: слот удалён/умер, а Приложение 1 продолжает писать пачку 2 (4 сообщения) в партиции!
        await fixture.ExecuteAsync("SELECT pg_drop_replication_slot($1)", slotName);
        (await fixture.GetSlotsAsync(slotName)).ShouldBeEmpty();

        await InsertPartitionedRowAsync(schema, eventsTopic, "b2-1", "batch2-msg1");
        await InsertPartitionedRowAsync(schema, eventsTopic, "b2-2", "batch2-msg2");
        await InsertPartitionedRowAsync(schema, eventsTopic, "b2-3", "batch2-msg3");
        await InsertPartitionedRowAsync(schema, eventsTopic, "b2-4", "batch2-msg4");

        // 4. 24/7 АВТО-ВОССТАНОВЛЕНИЕ (RecoveryMode = false, без ручного вмешательства!):
        //    сервис сам пересоздаёт failover-слот, вычитывает пачку 2 из партиций и снимает блокировку уборщика
        var progress2 = new ReplicationProgress();
        var publisher2 = new OutboxPublisher(
            fixture.TransactionalProducer,
            Options.Create(new ProcessorOptions { BatchSize = 50, CheckpointIntervalSeconds = 1 }),
            TimeProvider.System,
            metrics,
            progress2,
            checkpointStore,
            NullLogger<OutboxPublisher>.Instance,
            replicationOptions,
            storageOptions);
        var recoveryService2 = new PartitionRecoveryService(
            postgresOptions, storageOptions, partitionOptions, slotProvisioner, registryClient,
            publisher2, checkpointStore, progress2, TimeProvider.System, metrics,
            NullLogger<PartitionRecoveryService>.Instance);
        var startupChecks2 = new OutboxStartupChecks(
            slotProvisioner, pubVerifier, topicVerifier, checkpointStore, metrics,
            Options.Create(new CdcOutboxOptions { RecoveryMode = false }),
            NullLogger<OutboxStartupChecks>.Instance,
            storageOptions, partitionOptions, recoveryService2, registryClient, progress2, TimeProvider.System);

        await startupChecks2.RunAllAsync(CancellationToken.None);

        // Проверяем, что слот пересоздан именно с failover=true и замок уборщика снят
        var recreatedSlot = (await fixture.GetSlotsAsync(slotName)).Single();
        recreatedSlot.Plugin.ShouldBe("pgoutput");
        recreatedSlot.Failover.ShouldBe(true);

        var guardAfter = await registryClient.ReadGuardStateAsync(CancellationToken.None);
        guardAfter.ShouldNotBeNull();
        guardAfter!.RecoveryInProgress.ShouldBeFalse();

        // 5. Пишем пачку 3 (2 сообщения) ПОСЛЕ восстановления — проверяем, что живой поток слота подхватил её
        var checkpointBeforeBatch3 = progress2.CheckpointedPosition;
        var sentBeforeBatch3 = await ReadSentCountAsync(registry, slotName);
        await InsertPartitionedRowAsync(schema, eventsTopic, "b3-1", "batch3-msg1");
        await InsertPartitionedRowAsync(schema, eventsTopic, "b3-2", "batch3-msg2");

        var reader2 = new OutboxSlotReader(
            postgresOptions, replicationOptions, storageOptions,
            Options.Create(new ProcessorOptions { CheckpointIntervalSeconds = 1 }),
            progress2, TimeProvider.System, metrics, NullLogger<OutboxSlotReader>.Instance);
        await RunUntilDeliveredAndCheckpointedAsync(
            reader2,
            publisher2,
            progress2,
            registry,
            slotName,
            expectedSentTotal: sentBeforeBatch3 + 2,
            minCheckpointLsn: checkpointBeforeBatch3);

        var allConsumed = fixture.ConsumeTopic(eventsTopic, expected: 8, idleWindow: TimeSpan.FromSeconds(2));
        var uniqueBodies = allConsumed.Select(c => Encoding.UTF8.GetString(c.Message.Value)).ToHashSet();

        // Ноль потерь по всем трём пачкам (до аварии, во время аварии слота и после восстановления)!
        uniqueBodies.ShouldContain("batch1-msg1");
        uniqueBodies.ShouldContain("batch1-msg2");
        uniqueBodies.ShouldContain("batch2-msg1");
        uniqueBodies.ShouldContain("batch2-msg2");
        uniqueBodies.ShouldContain("batch2-msg3");
        uniqueBodies.ShouldContain("batch2-msg4");
        uniqueBodies.ShouldContain("batch3-msg1");
        uniqueBodies.ShouldContain("batch3-msg2");

        var metricsText = await MetricsExport.ExportAsync(registry);
        metricsText.ShouldContain($"outbox_partition_auto_recoveries_total{{slot=\"{slotName}\"}} 1");
    }

    private Task CreatePartitionedTableAsync(string schema, string table) =>
        fixture.CreateSchemaAsync(schema).ContinueWith(
            _ => fixture.ExecuteAsync(
                $"CREATE TABLE {schema}.{table} (" +
                "id bigint GENERATED ALWAYS AS IDENTITY NOT NULL, " +
                "topic varchar(255) NOT NULL, " +
                "key bytea NOT NULL, " +
                "body bytea NOT NULL, " +
                "headers varchar NOT NULL, " +
                "partition integer, " +
                "producer_key varchar(255), " +
                "created_at timestamptz NOT NULL DEFAULT now(), " +
                "outbox_partition integer NOT NULL DEFAULT 0, " +
                "PRIMARY KEY (created_at, id)) PARTITION BY RANGE (created_at)"),
            TaskScheduler.Default).Unwrap();

    private Task InsertPartitionedRowAsync(string schema, string topic, string key, string body) =>
        fixture.ExecuteAsync(
            $"INSERT INTO {schema}.messages (topic, key, body, headers, created_at) VALUES ($1, $2, $3, '{{}}', now())",
            topic,
            Encoding.UTF8.GetBytes(key),
            Encoding.UTF8.GetBytes(body));

    private async Task<ulong> ReadCurrentWalLsnAsync()
    {
        var rows = await fixture.QueryAsync("SELECT pg_current_wal_lsn()::text", r => r.GetString(0));
        return LsnText.TryParse(rows.Single())!.Value;
    }

    private static async Task<long> ReadSentCountAsync(CollectorRegistry registry, string slotName)
    {
        var prefix = $"outbox_processor_sent_messages_total{{slot=\"{slotName}\"}} ";
        var exported = await MetricsExport.ExportAsync(registry);
        foreach (var line in exported.Split('\n'))
        {
            if (line.StartsWith(prefix, StringComparison.Ordinal)
                && double.TryParse(line[prefix.Length..].Trim(), CultureInfo.InvariantCulture, out var value))
            {
                return (long)value;
            }
        }

        return 0;
    }

    private static async Task RunUntilDeliveredAndCheckpointedAsync(
        OutboxSlotReader reader,
        OutboxPublisher publisher,
        ReplicationProgress progress,
        CollectorRegistry registry,
        string slotName,
        long expectedSentTotal,
        ulong minCheckpointLsn)
    {
        var channel = Channel.CreateBounded<OutboxMessage>(128);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var readTask = reader.ReadAsync(channel.Writer, cts.Token);
        var publishTask = publisher.PublishAsync(channel.Reader, CancellationToken.None);

        var ok = await Wait.UntilAsync(async () =>
        {
            if (progress.CheckpointedPosition <= minCheckpointLsn)
            {
                return false;
            }

            return await ReadSentCountAsync(registry, slotName) >= expectedSentTotal;
        }, TimeSpan.FromSeconds(35));
        ok.ShouldBeTrue("сообщения не доставились и не зафиксировались в checkpoint за отведённый таймаут");

        await cts.CancelAsync();
        await readTask;
        channel.Writer.TryComplete();
        await publishTask;
    }
}
