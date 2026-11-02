using System.Text;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

[Collection("integration")]
public sealed class CheckpointStoreTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task Save_ThenReadLatest_ReturnsLatestBySlotKey()
    {
        var topicName = IntegrationFixture.NewName();
        await fixture.CreateTopicAsync(topicName);
        var store = CreateStore(topicName, "it_slot");

        await store.SaveAsync(0x123, CancellationToken.None);
        store.ReadLatest(CancellationToken.None).ShouldBe(0x123UL);

        await store.SaveAsync(0x456, CancellationToken.None);
        store.ReadLatest(CancellationToken.None).ShouldBe(0x456UL);
    }

    [Fact]
    public async Task ReadLatest_EmptyTopic_ReturnsNull()
    {
        var topicName = IntegrationFixture.NewName();
        await fixture.CreateTopicAsync(topicName);
        var store = CreateStore(topicName, "it_slot");

        store.ReadLatest(CancellationToken.None).ShouldBeNull();
    }

    [Fact]
    public async Task ReadLatest_IgnoresForeignSlotKeys()
    {
        var topicName = IntegrationFixture.NewName();
        await fixture.CreateTopicAsync(topicName);
        var store = CreateStore(topicName, "it_slot");
        await store.SaveAsync(0x100, CancellationToken.None);

        await ProduceRawAsync(topicName, "other_slot", "{\"slotName\":\"other_slot\",\"lsn\":\"0/999\"}");

        store.ReadLatest(CancellationToken.None).ShouldBe(0x100UL);
    }

    [Fact]
    public async Task ReadLatest_BrokenValue_FailsWithOffset()
    {
        var topicName = IntegrationFixture.NewName();
        await fixture.CreateTopicAsync(topicName);
        var store = CreateStore(topicName, "it_slot");

        await ProduceRawAsync(topicName, "it_slot", "не json");

        var error = Should.Throw<InvalidOperationException>(
            () => store.ReadLatest(CancellationToken.None));
        error.Message.ShouldContain("не разбирается");
    }

    private CheckpointStore CreateStore(string topicName, string slotName) =>
        new(
            fixture.Producer,
            fixture.BuildOptions<KafkaOptions>(
                KafkaOptions.ConfigurationSection,
                (KafkaOptions.ConfigurationSection + ":CheckpointTopic", topicName)),
            Options.Create(new ReplicationOptions { SlotName = slotName }),
            TimeProvider.System,
            NullLogger<CheckpointStore>.Instance);

    private async Task ProduceRawAsync(string topicName, string key, string value) =>
        await fixture.Producer.ProduceAsync(
            new TopicPartition(topicName, new Partition(0)),
            new Message<byte[], byte[]> { Key = Encoding.UTF8.GetBytes(key), Value = Encoding.UTF8.GetBytes(value) },
            CancellationToken.None);
}
