using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using Confluent.Kafka;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

/// <summary>
/// Живые проверки арбитража Active-Passive: назначение единственной партиции
/// контрольной группы ровно одному члену пары (leader), освобождение партиции
/// при Close (LeaveGroup — мгновенный rebalance) и перехват резерва.
/// Машина состояний лидерства поверх этих событий покрыта unit-тестами координатора.
/// </summary>
[Collection("integration")]
public sealed class ArbiterTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task ControlGroup_SinglePartition_AssignedToExactlyOneMember()
    {
        var topicName = IntegrationFixture.NewName();
        await fixture.CreateTopicAsync(topicName);
        var kafka = fixture.BuildOptions<KafkaOptions>(
            KafkaOptions.ConfigurationSection,
            (KafkaOptions.ConfigurationSection + ":CheckpointTopic", topicName),
            (KafkaOptions.ConfigurationSection + ":ControlGroupId", $"it_arbiter_{Guid.NewGuid():N}"))
            .Value;

        using var first = BuildConsumer(kafka);
        first.Subscribe(topicName);
        WaitForAssignment(first, TimeSpan.FromSeconds(30)).ShouldBeTrue();

        // резерв подключается к группе; оба опрашиваются — равновесие rebalance:
        // единственная партиция ровно у одного члена пары (лидера)
        using var second = BuildConsumer(kafka);
        second.Subscribe(topicName);
        var settled = false;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            _ = first.Consume(TimeSpan.FromMilliseconds(500));
            _ = second.Consume(TimeSpan.FromMilliseconds(500));
            if (AssignedPartitions(first, second) == 1)
            {
                settled = true;
                break;
            }
        }

        settled.ShouldBeTrue();
        // у пары суммарно назначена ровно одна партиция — у лидера
        AssignedPartitions(first, second).ShouldBe(1);
        var leader = first.Assignment.Count == 1 ? first : second;

        // graceful остановка лидера (Close = LeaveGroup): партиция мгновенно
        // уходит резерву — без ожидания session timeout
        leader.Close();
        var standby = leader == first ? second : first;
        WaitForAssignment(standby, TimeSpan.FromSeconds(30)).ShouldBeTrue();
        standby.Assignment.ShouldHaveSingleItem().Topic.ShouldBe(topicName);
    }

    private static int AssignedPartitions(params IConsumer<byte[], byte[]>[] consumers) =>
        consumers.Sum(consumer => consumer.Assignment.Count);

    private static IConsumer<byte[], byte[]> BuildConsumer(KafkaOptions kafka) =>
        new ConsumerBuilder<byte[], byte[]>(OutboxProducerConfig.BuildControlConsumerConfig(kafka)).Build();

    private static bool WaitForAssignment(IConsumer<byte[], byte[]> consumer, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            _ = consumer.Consume(TimeSpan.FromSeconds(1));
            if (consumer.Assignment.Count > 0)
            {
                return true;
            }
        }

        return consumer.Assignment.Count > 0;
    }
}
