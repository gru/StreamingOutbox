using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Replication;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Kafka;

/// <summary>
/// Checkpoint-стор в Kafka: позиция слота сохраняется записью в служебный
/// топик (ключ — имя слота, партиция 0) и считается сохранённой только после
/// подтверждения записи брокером.
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
    private readonly TimeSpan _readTimeout = TimeSpan.FromSeconds(kafkaOptions.Value.CheckpointReadTimeoutSeconds);
    private readonly TimeSpan _pollTimeout = TimeSpan.FromSeconds(kafkaOptions.Value.CheckpointPollTimeoutSeconds);

    /// <summary>
    /// Сохранить checkpoint позиции слота; задача завершается после
    /// подтверждения записи брокером.
    /// </summary>
    /// <param name="lsn">Позиция слота.</param>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Задача сохранения.</returns>
    public async Task SaveAsync(ulong lsn, CancellationToken cancellationToken)
    {
        var message = new Message<byte[], byte[]>
        {
            Key = Encoding.UTF8.GetBytes(_slotName),
            Value = EncodeValue(new CheckpointRecord(_slotName, LsnText.Format(lsn), timeProvider.GetUtcNow().UtcDateTime)),
        };
        await producer.ProduceAsync(
            new TopicPartition(_kafka.CheckpointTopic, new Partition(0)),
            message,
            cancellationToken);
        logger.LogInformation(
            "Checkpoint слота '{SlotName}' сохранён ({Topic}): LSN {Lsn}.",
            _slotName,
            _kafka.CheckpointTopic,
            LsnText.Format(lsn));
    }

    /// <summary>
    /// Прочитать последний сохранённый checkpoint слота; null, если сохранений не было.
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

        // компакция хранит по одной записи на ключ — последняя запись ключа слота
        // и есть текущее значение
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
            throw new InvalidOperationException(
                $"Запись checkpoint (offset {offset}) не разбирается: '{Encoding.UTF8.GetString(value)}'.",
                exception);
        }

        if (LsnText.TryParse(record?.Lsn) is not { } lsn)
        {
            throw new InvalidOperationException(
                $"Запись checkpoint (offset {offset}) не содержит корректный LSN: '{record?.Lsn ?? "<пусто>"}'.");
        }

        return lsn;
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
