using Confluent.Kafka;

namespace CdcOutbox.UnitTests;

internal sealed class FakeProducer(Func<Message<byte[], byte[]>, int, Task<DeliveryResult<byte[], byte[]>>>? deliver = null)
    : IProducer<byte[], byte[]>
{
    private readonly Func<Message<byte[], byte[]>, int, Task<DeliveryResult<byte[], byte[]>>> _deliver =
        deliver ?? (static (_, _) => Task.FromResult(new DeliveryResult<byte[], byte[]> { Status = PersistenceStatus.Persisted }));

    private int _attempts;
    private bool _initialized;

    /// <summary>
    /// Фабрика исключения CommitTransaction (имитация fencing или сбоя фиксации
    /// транзакции); вызывается на каждом commit.
    /// </summary>
    public Func<Exception?>? OnCommit { get; set; }

    /// <summary>
    /// Вызван ли Dispose (сервис периода лидерства освобождает продюсер скоупом).
    /// </summary>
    public bool Disposed { get; private set; }

    public List<string> TransactionCalls { get; } = [];

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
        if (_initialized)
        {
            // как реальный клиент: повторный InitTransactions на готовом
            // продюсере невалиден — продюсер пересоздаётся на каждый период лидерства
            throw new KafkaException(new Error(ErrorCode.Local_State, "Operation not valid in state Ready"));
        }

        _initialized = true;
        TransactionCalls.Add("init");
    }

    void IProducer<byte[], byte[]>.BeginTransaction()
    {
        TransactionCalls.Add("begin");
    }

    void IProducer<byte[], byte[]>.CommitTransaction(TimeSpan timeout)
    {
        TransactionCalls.Add("commit");
        if (OnCommit?.Invoke() is { } failure)
        {
            throw failure;
        }
    }

    void IProducer<byte[], byte[]>.CommitTransaction()
    {
        TransactionCalls.Add("commit");
        if (OnCommit?.Invoke() is { } failure)
        {
            throw failure;
        }
    }

    void IProducer<byte[], byte[]>.AbortTransaction(TimeSpan timeout)
    {
        TransactionCalls.Add("abort");
    }

    void IProducer<byte[], byte[]>.AbortTransaction()
    {
        TransactionCalls.Add("abort");
    }

    void IProducer<byte[], byte[]>.SendOffsetsToTransaction(
        IEnumerable<TopicPartitionOffset> offsets,
        IConsumerGroupMetadata groupMetadata,
        TimeSpan timeout)
    {
    }

    void IDisposable.Dispose()
    {
        Disposed = true;
    }
}
