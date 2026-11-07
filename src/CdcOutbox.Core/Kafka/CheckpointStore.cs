using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Processing;
using CdcOutbox.Core.Replication;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Kafka;

/// <summary>
/// Хранит checkpoint позиции слота в топике Kafka.
/// </summary>
public sealed class CheckpointStore(
    IProducer<byte[], byte[]> producer,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<ReplicationOptions> replicationOptions,
    TimeProvider timeProvider,
    ILogger<CheckpointStore> logger)
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
    };

    private readonly KafkaOptions _kafka = kafkaOptions.Value;
    private readonly string _slotName = replicationOptions.Value.SlotName;
    private readonly byte[] _slotKey = Encoding.UTF8.GetBytes(replicationOptions.Value.SlotName);
    private readonly TimeSpan _readTimeout = TimeSpan.FromSeconds(kafkaOptions.Value.CheckpointReadTimeoutSeconds);
    private readonly TimeSpan _pollTimeout = TimeSpan.FromSeconds(kafkaOptions.Value.CheckpointPollTimeoutSeconds);

    /// <summary>
    /// Добавляет checkpoint-запись в открытую транзакцию продюсера.
    /// </summary>
    /// <param name="lsn">Позиция слота.</param>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Задача производства записи.</returns>
    public async Task ProduceInTransactionAsync(ulong lsn, CancellationToken cancellationToken)
    {
        var record = new CheckpointRecord(_slotName, LsnText.Format(lsn), timeProvider.GetUtcNow().UtcDateTime);
        var message = new Message<byte[], byte[]>
        {
            Key = _slotKey,
            Value = EncodeValue(record),
        };
        var partition = new TopicPartition(_kafka.CheckpointTopic, new Partition(0));
        await producer.ProduceAsync(partition, message, cancellationToken);
    }

    /// <summary>
    /// Сохраняет checkpoint отдельной транзакцией продюсера.
    /// </summary>
    /// <param name="lsn">Позиция слота.</param>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Задача сохранения.</returns>
    public async Task SaveTransactionAsync(ulong lsn, CancellationToken cancellationToken)
    {
        producer.BeginTransaction();
        try
        {
            await ProduceInTransactionAsync(lsn, cancellationToken);
            producer.CommitTransaction(OutboxProducerConfig.TransactionOperationTimeout);
        }
        catch (Exception)
        {
            TryAbort();
            throw;
        }

        logger.LogDebug(
            "Checkpoint слота '{SlotName}' сохранён транзакцией ({Topic}): LSN {Lsn}.",
            _slotName,
            _kafka.CheckpointTopic,
            LsnText.Format(lsn));
    }

    /// <summary>
    /// Возвращает последний сохранённый checkpoint слота.
    /// </summary>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Позиция последнего checkpoint; null, если сохранений не было.</returns>
    public ulong? ReadLatest(CancellationToken cancellationToken)
    {
        using var consumer = new ConsumerBuilder<byte[], byte[]>(
            OutboxProducerConfig.BuildCheckpointConsumerConfig(_kafka)).Build();
        var partition = new TopicPartition(_kafka.CheckpointTopic, new Partition(0));
        consumer.Assign(new TopicPartitionOffset(partition, Offset.Beginning));

        var deadline = timeProvider.GetUtcNow() + _readTimeout;
        ConsumeResult<byte[], byte[]>? latest = null;
        while (timeProvider.GetUtcNow() < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ConsumeResult<byte[], byte[]>? result;
            try
            {
                result = consumer.Consume(_pollTimeout);
            }
            catch (ConsumeException exception)
            {
                throw new InvalidOperationException(
                    $"Чтение checkpoint-стора '{_kafka.CheckpointTopic}' не удалось: {exception.Error.Reason}.",
                    exception);
            }

            if (result is null)
            {
                if (ReachedEnd(consumer, partition))
                {
                    break;
                }

                continue;
            }

            if (IsCheckpointKey(result.Message.Key))
            {
                latest = result;
            }
        }

        // cleanup.policy=compact оставляет для ключа последнюю запись — она и есть текущий checkpoint
        return latest is null ? null : DecodeValue(latest.Message.Value, latest.Offset.Value);
    }

    internal static byte[] EncodeValue(CheckpointRecord record) =>
        JsonSerializer.SerializeToUtf8Bytes(record, JsonOptions);

    internal static ulong DecodeValue(byte[] value, long offset)
    {
        CheckpointRecord? record;
        try
        {
            record = JsonSerializer.Deserialize<CheckpointRecord>(value, JsonOptions);
        }
        catch (JsonException exception)
        {
            throw new StartupCheckException(
                $"Запись checkpoint (offset {offset}) не разбирается: '{Encoding.UTF8.GetString(value)}'.",
                exception);
        }

        if (LsnText.TryParse(record?.Lsn) is not { } lsn)
        {
            throw new StartupCheckException(
                $"Запись checkpoint (offset {offset}) не содержит корректный LSN: '{record?.Lsn ?? "<пусто>"}'.");
        }

        return lsn;
    }

    private void TryAbort()
    {
        try
        {
            producer.AbortTransaction(OutboxProducerConfig.TransactionOperationTimeout);
        }
        catch (Exception exception)
        {
            // откат у заблокированного fencing/останавливаемого продюсера может не выполниться —
            // запись не зафиксирована, ошибка первопричины важнее
            logger.LogDebug(exception, "Откат незакрытой транзакции checkpoint не выполнен.");
        }
    }

    private bool IsCheckpointKey(byte[]? key) =>
        key is not null && Encoding.UTF8.GetString(key) == _slotName;

    private static bool ReachedEnd(IConsumer<byte[], byte[]> consumer, TopicPartition partition)
    {
        var watermarks = consumer.GetWatermarkOffsets(partition);
        var position = consumer.Position(partition);
        return position == Offset.Unset ? watermarks.Low == watermarks.High : position >= watermarks.High;
    }

    internal sealed record CheckpointRecord(string SlotName, string Lsn, DateTime SavedAtUtc);
}
