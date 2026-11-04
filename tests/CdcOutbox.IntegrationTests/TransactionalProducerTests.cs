using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using Confluent.Kafka;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

/// <summary>
/// Платформенное допущение пересоздания продюсера на период лидерства:
/// повторный InitTransactions на готовом экземпляре невалиден, свежий
/// экземпляр с тем же transactional.id инициализируется — бамп эпохи
/// в координаторе транзакций fenced'ит прежний экземпляр.
/// </summary>
[Collection("integration")]
public sealed class TransactionalProducerTests(IntegrationFixture fixture)
{
    // один transactional.id на тест: оба экземпляра строятся от одного слота
    private readonly string _slotName = $"it-reinit-{Guid.NewGuid():N}";

    [Fact]
    public void InitTransactions_SecondCallOnReadyInstance_ThrowsInvalidState()
    {
        using var producer = BuildProducer();
        producer.InitTransactions(TimeSpan.FromSeconds(30));

        // повторное лидерство тем же процессом без пересоздания продюсера —
        // именно это роняло хост до пересоздания продюсера на каждый период лидерства
        var error = Should.Throw<KafkaException>(() => producer.InitTransactions(TimeSpan.FromSeconds(30)));
        error.Message.ShouldContain("Operation not valid in state");
    }

    [Fact]
    public async Task InitTransactions_FreshInstance_SameTransactionalId_SucceedsAndFencesPrevious()
    {
        var topicName = IntegrationFixture.NewName();
        await fixture.CreateTopicAsync(topicName);
        var first = BuildProducer();
        try
        {
            first.InitTransactions(TimeSpan.FromSeconds(30));
            first.BeginTransaction();
            await first.ProduceAsync(
                new TopicPartition(topicName, new Partition(0)),
                new Message<byte[], byte[]>(),
                CancellationToken.None);

            // следующий период лидерства: новый экземпляр, тот же transactional.id — бамп
            // эпохи; открытая транзакция прежнего экземпляра (механика
            // «опоздавшего commit») отклоняется координатором fencing'ом
            using var second = BuildProducer();
            second.InitTransactions(TimeSpan.FromSeconds(30));

            Should.Throw<KafkaException>(() => first.CommitTransaction(TimeSpan.FromSeconds(30)));
        }
        finally
        {
            first.Dispose();
        }
    }

    private IProducer<byte[], byte[]> BuildProducer() =>
        new ProducerBuilder<byte[], byte[]>(
            OutboxProducerConfig.Build(
                fixture.BuildOptions<KafkaOptions>(KafkaOptions.ConfigurationSection).Value,
                _slotName)).Build();
}
