using Confluent.Kafka;

namespace CdcOutbox.UnitTests;

internal sealed class FakeProducer(Func<Message<byte[], byte[]>, int, Task<DeliveryResult<byte[], byte[]>>>? deliver = null)
    : IProducer<byte[], byte[]>
{
    private readonly Func<Message<byte[], byte[]>, int, Task<DeliveryResult<byte[], byte[]>>> _deliver =
        deliver ?? (static (_, _) => Task.FromResult(new DeliveryResult<byte[], byte[]> { Status = PersistenceStatus.Persisted }));

    private int _attempts;

    public List<(string Topic, Partition Partition, Message<byte[], byte[]> Message)> Produced { get; } = [];

    string IClient.Name => "fake";

    Handle IClient.Handle => throw new NotSupportedException("Дескриптор нативного клиента у фейка недоступен.");

    int IClient.AddBrokers(string brokers) => 0;

    void IClient.SetSaslCredentials(string username, string password)
    {
    }

    int IProducer<byte[], byte[]>.Poll(TimeSpan timeout) => 0;

    int IProducer<byte[], byte[]>.Flush(TimeSpan timeout) => 0;

    void IProducer<byte[], byte[]>.Flush(CancellationToken cancellationToken)
    {
    }

    Task<DeliveryResult<byte[], byte[]>> IProducer<byte[], byte[]>.ProduceAsync(
        string topic,
        Message<byte[], byte[]> message,
        CancellationToken cancellationToken)
    {
        Produced.Add((topic, Partition.Any, message));
        return _deliver(message, _attempts++);
    }

    Task<DeliveryResult<byte[], byte[]>> IProducer<byte[], byte[]>.ProduceAsync(
        TopicPartition topicPartition,
        Message<byte[], byte[]> message,
        CancellationToken cancellationToken)
    {
        Produced.Add((topicPartition.Topic, topicPartition.Partition, message));
        return _deliver(message, _attempts++);
    }

    void IProducer<byte[], byte[]>.Produce(
        string topic,
        Message<byte[], byte[]> message,
        Action<DeliveryReport<byte[], byte[]>>? deliveryHandler) =>
        throw new NotSupportedException("Синхронная отправка публикатором не используется.");

    void IProducer<byte[], byte[]>.Produce(
        TopicPartition topicPartition,
        Message<byte[], byte[]> message,
        Action<DeliveryReport<byte[], byte[]>>? deliveryHandler) =>
        throw new NotSupportedException("Синхронная отправка публикатором не используется.");

    void IProducer<byte[], byte[]>.InitTransactions(TimeSpan timeout)
    {
    }

    void IProducer<byte[], byte[]>.BeginTransaction()
    {
    }

    void IProducer<byte[], byte[]>.CommitTransaction(TimeSpan timeout)
    {
    }

    void IProducer<byte[], byte[]>.CommitTransaction()
    {
    }

    void IProducer<byte[], byte[]>.AbortTransaction(TimeSpan timeout)
    {
    }

    void IProducer<byte[], byte[]>.AbortTransaction()
    {
    }

    void IProducer<byte[], byte[]>.SendOffsetsToTransaction(
        IEnumerable<TopicPartitionOffset> offsets,
        IConsumerGroupMetadata groupMetadata,
        TimeSpan timeout)
    {
    }

    void IDisposable.Dispose()
    {
    }
}
