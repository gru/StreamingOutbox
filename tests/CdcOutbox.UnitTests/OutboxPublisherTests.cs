using System.Text;
using System.Threading.Channels;
using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Redundancy;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Prometheus;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class OutboxPublisherTests
{
    private const string SlotName = "outbox_unit_slot";

    [Fact]
    public async Task Publish_SendsKeyBodyTopicAndCreateTimeTimestamp()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var producer = new FakeProducer();
        var createdAt = new DateTime(2026, 10, 23, 8, 0, 0, DateTimeKind.Utc);
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(CreateMessage(id: 1, key: "k1", headers: "{}", createdAt: createdAt));
        await channel.Writer.WriteAsync(CreateMessage(id: 2, key: "k2", headers: "{}", createdAt: createdAt));
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await CreatePublisher(producer, metrics).PublishAsync(channel.Reader, cts.Token);

        producer.Produced.Count.ShouldBe(2);
        var first = producer.Produced[0];
        first.Topic.ShouldBe("orders.events");
        first.Partition.ShouldBe(Partition.Any);
        first.Message.Key.ShouldBe(Encoding.UTF8.GetBytes("k1"));
        first.Message.Value.ShouldBe(Encoding.UTF8.GetBytes("body-k1"));
        first.Message.Timestamp.Type.ShouldBe(TimestampType.CreateTime);
        first.Message.Timestamp.UtcDateTime.ShouldBe(createdAt);
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_sent_messages_total{slot=\"it_slot\"} 2");
        text.ShouldContain("outbox_processor_pending_events_current{slot=\"it_slot\"} 0");
        text.ShouldContain("outbox_processor_publish_latency_seconds_count{slot=\"it_slot\"} 2");
        text.ShouldContain("outbox_message_age_seconds_count{slot=\"it_slot\"} 2");
    }

    [Fact]
    public async Task Publish_RoutesExplicitPartition()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var producer = new FakeProducer();
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(CreateMessage(id: 1, key: "k", partition: 2));
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await CreatePublisher(producer, metrics).PublishAsync(channel.Reader, cts.Token);

        producer.Produced.ShouldHaveSingleItem().Partition.ShouldBe(new Partition(2));
    }

    [Fact]
    public async Task Publish_MapsHeadersFromJson()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var producer = new FakeProducer();
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(
            CreateMessage(id: 1, key: "k", headers: "{\"trace-id\":\"abc\",\"source\":\"it\"}"));
        await channel.Writer.WriteAsync(CreateMessage(id: 2, key: "k", headers: "{}"));
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await CreatePublisher(producer, metrics).PublishAsync(channel.Reader, cts.Token);

        var mapped = producer.Produced[0].Message.Headers.ShouldNotBeNull();
        mapped.Count.ShouldBe(2);
        Encoding.UTF8.GetString(mapped.GetLastBytes("trace-id")).ShouldBe("abc");
        Encoding.UTF8.GetString(mapped.GetLastBytes("source")).ShouldBe("it");
        producer.Produced[1].Message.Headers.ShouldNotBeNull().Count.ShouldBe(0);
    }

    [Fact]
    public async Task Publish_BrokenHeaders_StopsNewMessagesAndCountsErrors()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var producer = new FakeProducer();
        var logger = new RecordingLogger<OutboxPublisher>();
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(CreateMessage(id: 1, key: "broken", headers: "не json"));
        await channel.Writer.WriteAsync(CreateMessage(id: 2, key: "valid", headers: "{}"));
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var publishing = CreatePublisher(producer, metrics, logger: logger)
            .PublishAsync(channel.Reader, cts.Token);
        var logged = await TestWait.WaitForAsync(
            () => logger.Entries.Any(entry => entry.Level == LogLevel.Error && entry.Message.Contains("Заголовки")),
            TimeSpan.FromSeconds(10));

        logged.ShouldBeTrue();
        await cts.CancelAsync();
        await SwallowAsync(publishing);

        producer.Produced.Count.ShouldBe(1);
        Encoding.UTF8.GetString(producer.Produced[0].Message.Key).ShouldBe("valid");
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_sent_messages_total{slot=\"it_slot\"} 1");
        var errorLine = text
            .Split('\n')
            .Single(line => line.StartsWith("outbox_processor_errors_total{", StringComparison.Ordinal));
        errorLine.ShouldNotEndWith(" 0");
    }

    [Fact]
    public async Task Publish_FailedDelivery_GoesToRetryAndRepublished()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var attemptsByMessage = new Dictionary<string, int>();
        var producer = new FakeProducer((message, _) =>
        {
            var key = Encoding.UTF8.GetString(message.Key);
            attemptsByMessage.TryGetValue(key, out var attempts);
            attemptsByMessage[key] = attempts + 1;
            return key == "2" && attempts == 0
                ? Task.FromException<DeliveryResult<byte[], byte[]>>(
                    new ProduceException<byte[], byte[]>(
                        new Error(ErrorCode.MsgSizeTooLarge),
                        new DeliveryResult<byte[], byte[]>()))
                : Task.FromResult(new DeliveryResult<byte[], byte[]> { Status = PersistenceStatus.Persisted });
        });
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(CreateMessage(id: 1, key: "1"));
        await channel.Writer.WriteAsync(CreateMessage(id: 2, key: "2"));
        await channel.Writer.WriteAsync(CreateMessage(id: 3, key: "3"));
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await CreatePublisher(producer, metrics).PublishAsync(channel.Reader, cts.Token);

        producer.Produced
            .Select(produced => Encoding.UTF8.GetString(produced.Message.Key))
            .ToList()
            .ShouldBe(new List<string> { "1", "2", "3", "2" });
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_sent_messages_total{slot=\"it_slot\"} 3");
        text.ShouldContain("outbox_processor_pending_events_current{slot=\"it_slot\"} 0");
        text.ShouldContain("outbox_processor_errors_total{slot=\"it_slot\"} 1");
    }

    [Fact]
    public async Task Publish_ProduceThrowsSync_RetriesBatchWithoutLoss()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var producer = new FakeProducer((_, attempt) => attempt < 3
            ? throw new IOException("брокер недоступен")
            : Task.FromResult(new DeliveryResult<byte[], byte[]> { Status = PersistenceStatus.Persisted }));
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(CreateMessage(id: 1, key: "1"));
        await channel.Writer.WriteAsync(CreateMessage(id: 2, key: "2"));
        await channel.Writer.WriteAsync(CreateMessage(id: 3, key: "3"));
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await CreatePublisher(producer, metrics).PublishAsync(channel.Reader, cts.Token);

        producer.Produced.Count.ShouldBe(6);
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_sent_messages_total{slot=\"it_slot\"} 3");
        text.ShouldContain("outbox_processor_errors_total{slot=\"it_slot\"} 3");
    }

    [Fact]
    public async Task Publish_CompletedEmptyChannel_Exits()
    {
        var metrics = new OutboxMetrics(new CollectorRegistry(), slotName: "it_slot");
        var producer = new FakeProducer();
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var publishing = CreatePublisher(producer, metrics).PublishAsync(channel.Reader, cts.Token);
        await publishing.WaitAsync(TimeSpan.FromSeconds(10));

        publishing.IsCompletedSuccessfully.ShouldBeTrue();
    }

    [Fact]
    public async Task Publish_CancellationWhileIdle_CompletesWithoutError()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var producer = new FakeProducer();
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var publishing = CreatePublisher(producer, metrics).PublishAsync(channel.Reader, cts.Token);
        await Task.Delay(200);
        await cts.CancelAsync();
        await SwallowAsync(publishing);

        var text = await MetricsExport.ExportAsync(registry);
        text.Split('\n')
            .Where(line => line.StartsWith("outbox_processor_errors_total{", StringComparison.Ordinal))
            .ShouldNotContain(line => !line.EndsWith(" 0"));
    }

    [Fact]
    public async Task Publish_CompletedChannel_SavesFinalCheckpointWithWatermark()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var producer = new FakeProducer();
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(CreateMessage(id: 1, key: "1", transactionEndLsn: null));
        await channel.Writer.WriteAsync(CreateMessage(id: 2, key: "2", transactionEndLsn: 0x200));
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await CreatePublisher(producer, metrics, checkpointIntervalSeconds: 3600)
            .PublishAsync(channel.Reader, cts.Token);

        var checkpoint = producer.Produced.Single(entry => entry.Topic == "cdc-checkpoints");
        checkpoint.Partition.ShouldBe(new Partition(0));
        Encoding.UTF8.GetString(checkpoint.Message.Key).ShouldBe(SlotName);
        CheckpointStore.DecodeValue(checkpoint.Message.Value, offset: 0).ShouldBe(0x200UL);
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_checkpoint_age_seconds{slot=\"it_slot\"} 0");
    }

    [Fact]
    public async Task Publish_CheckpointCadence_SavesWhileChannelOpen()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var producer = new FakeProducer();
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(CreateMessage(id: 1, key: "1", transactionEndLsn: 0x100));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var publishing = CreatePublisher(producer, metrics, checkpointIntervalSeconds: 1)
            .PublishAsync(channel.Reader, cts.Token);
        var saved = await TestWait.WaitForAsync(
            () => producer.Produced.Any(entry => entry.Topic == "cdc-checkpoints"),
            TimeSpan.FromSeconds(10));

        saved.ShouldBeTrue();
        channel.Writer.Complete();
        await publishing;
    }

    [Fact]
    public async Task Publish_FailedDelivery_CheckpointSavedAfterRetrySucceeds()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var attemptsByMessage = new Dictionary<string, int>();
        var producer = new FakeProducer((message, _) =>
        {
            var key = Encoding.UTF8.GetString(message.Key);
            attemptsByMessage.TryGetValue(key, out var attempts);
            attemptsByMessage[key] = attempts + 1;
            return key == "2" && attempts == 0
                ? Task.FromException<DeliveryResult<byte[], byte[]>>(
                    new ProduceException<byte[], byte[]>(
                        new Error(ErrorCode.MsgSizeTooLarge),
                        new DeliveryResult<byte[], byte[]>()))
                : Task.FromResult(new DeliveryResult<byte[], byte[]> { Status = PersistenceStatus.Persisted });
        });
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(CreateMessage(id: 1, key: "1", transactionEndLsn: null));
        await channel.Writer.WriteAsync(CreateMessage(id: 2, key: "2", transactionEndLsn: 0x200));
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await CreatePublisher(producer, metrics, checkpointIntervalSeconds: 3600)
            .PublishAsync(channel.Reader, cts.Token);

        var checkpoint = producer.Produced.Single(entry => entry.Topic == "cdc-checkpoints");
        CheckpointStore.DecodeValue(checkpoint.Message.Value, offset: 0).ShouldBe(0x200UL);
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_errors_total{slot=\"it_slot\"} 1");
    }

    [Fact]
    public async Task Publish_CommitFenced_ThrowsLeadershipLost()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var producer = new FakeProducer { OnCommit = static () => new KafkaException(new Error(ErrorCode.ProducerFenced)) };
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(CreateMessage(id: 1, key: "1", transactionEndLsn: 0x100));
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var publishing = CreatePublisher(producer, metrics).PublishAsync(channel.Reader, cts.Token);
        await TestAssert.AssertThrowsAsync<LeadershipLostException>(publishing);

        producer.TransactionCalls.ShouldContain("abort");
        // fencing — потеря лидерства, а не ошибка процессора: RegisterError
        // не вызывался — серии-значения errors_total в экспорте нет
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldNotContain("outbox_processor_errors_total{");
    }

    [Fact]
    public async Task Publish_CommitFailsOnce_RetriesWholeBatch()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var commits = 0;
        var producer = new FakeProducer
        {
            OnCommit = () => Interlocked.Increment(ref commits) == 1
                ? new IOException("координатор транзакций недоступен")
                : null,
        };
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        await channel.Writer.WriteAsync(CreateMessage(id: 1, key: "1"));
        await channel.Writer.WriteAsync(CreateMessage(id: 2, key: "2", transactionEndLsn: 0x100));
        channel.Writer.Complete();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await CreatePublisher(producer, metrics, retryDelaySeconds: 1, checkpointIntervalSeconds: 3600)
            .PublishAsync(channel.Reader, cts.Token);

        // первый commit не прошёл — весь батч повторён заново (записи прерванной
        // транзакции невидимы read_committed-читателям), второй зафиксировал события и checkpoint
        producer.Produced.Count(entry => entry.Topic == "orders.events").ShouldBe(4);
        var checkpoints = producer.Produced.Where(entry => entry.Topic == "cdc-checkpoints").ToList();
        checkpoints.Count.ShouldBe(2);
        CheckpointStore.DecodeValue(checkpoints[^1].Message.Value, offset: 0).ShouldBe(0x100UL);
        var text = await MetricsExport.ExportAsync(registry);
        text.ShouldContain("outbox_processor_errors_total{slot=\"it_slot\"} 1");
        text.ShouldContain("outbox_processor_pending_events_current{slot=\"it_slot\"} 0");
    }

    [Fact]
    public async Task Publish_IdleCheckpoint_SavesUnsavedProgressWithoutNewEvents()
    {
        var registry = new CollectorRegistry();
        var metrics = new OutboxMetrics(registry, slotName: "it_slot");
        var producer = new FakeProducer();
        var channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(16));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var publishing = CreatePublisher(producer, metrics, checkpointIntervalSeconds: 1)
            .PublishAsync(channel.Reader, cts.Token);

        // первая транзакция несёт checkpoint сразу (первое сохранение — с первым прогрессом)
        await channel.Writer.WriteAsync(CreateMessage(id: 1, key: "1", transactionEndLsn: 0x100));
        var firstSaved = await TestWait.WaitForAsync(
            () => producer.Produced.Count(entry => entry.Topic == "cdc-checkpoints") == 1,
            TimeSpan.FromSeconds(10));
        firstSaved.ShouldBeTrue();

        // вторая фиксируется без checkpoint (каденс не истёк), затем простой:
        // несохранённый прогресс уходит checkpoint-only транзакцией по каденцу
        await channel.Writer.WriteAsync(CreateMessage(id: 2, key: "2", transactionEndLsn: 0x200));
        var idleSaved = await TestWait.WaitForAsync(
            () => producer.Produced
                .Where(entry => entry.Topic == "cdc-checkpoints")
                .Any(entry => CheckpointStore.DecodeValue(entry.Message.Value, offset: 0) == 0x200UL),
            TimeSpan.FromSeconds(10));
        idleSaved.ShouldBeTrue();

        channel.Writer.Complete();
        await publishing;
        producer.Produced.Count(entry => entry.Topic == "cdc-checkpoints").ShouldBe(2);
    }

    private static OutboxPublisher CreatePublisher(
        FakeProducer producer,
        OutboxMetrics metrics,
        int batchSize = 500,
        int retryDelaySeconds = 1,
        int checkpointIntervalSeconds = 3600,
        ILogger<OutboxPublisher>? logger = null) =>
        new(
            producer,
            Options.Create(new ProcessorOptions
            {
                BatchSize = batchSize,
                RetryDelaySeconds = retryDelaySeconds,
                CheckpointIntervalSeconds = checkpointIntervalSeconds,
            }),
            TimeProvider.System,
            metrics,
            new ReplicationProgress(),
            CreateCheckpointStore(producer),
            logger ?? NullLogger<OutboxPublisher>.Instance);

    private static CheckpointStore CreateCheckpointStore(FakeProducer producer) =>
        new(
            producer,
            Options.Create(new KafkaOptions()),
            Options.Create(new ReplicationOptions { SlotName = SlotName }),
            TimeProvider.System,
            NullLogger<CheckpointStore>.Instance);

    private static OutboxMessage CreateMessage(
        long id,
        string key,
        string headers = "{}",
        int? partition = null,
        DateTime? createdAt = null,
        ulong? transactionEndLsn = null) =>
        new()
        {
            Id = id,
            Topic = "orders.events",
            Key = Encoding.UTF8.GetBytes(key),
            Body = Encoding.UTF8.GetBytes($"body-{key}"),
            Headers = headers,
            Partition = partition,
            CreatedAt = createdAt ?? DateTime.UtcNow,
            TransactionEndLsn = transactionEndLsn,
        };

    private static async Task SwallowAsync(Task task)
    {
        try
        {
            await task;
        }
        catch (Exception)
        {
        }
    }
}
