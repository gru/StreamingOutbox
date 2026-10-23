using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

[Collection("integration")]
public sealed class CheckpointTopicVerifierTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task Verify_ExistingTopic_Passes()
    {
        var topicName = $"it_{Guid.NewGuid():N}";
        await fixture.AdminClient.CreateTopicsAsync(
            new[] { new TopicSpecification { Name = topicName, NumPartitions = 1, ReplicationFactor = 1 } });
        await WaitUntilTopicVisibleAsync(topicName);
        var logger = new RecordingLogger<CheckpointTopicVerifier>();

        CreateVerifier(topicName, logger).Verify();

        logger.Entries.ShouldContain(entry =>
            entry.Level == LogLevel.Information && entry.Message.Contains(topicName) && entry.Message.Contains("существует"));
    }

    [Fact]
    public void Verify_MissingTopic_Fails()
    {
        var topicName = $"it_{Guid.NewGuid():N}";
        var logger = new RecordingLogger<CheckpointTopicVerifier>();

        var error = Should.Throw<InvalidOperationException>(() => CreateVerifier(topicName, logger).Verify());

        error.Message.ShouldContain(topicName);
        error.Message.ShouldContain("отсутствует");
    }

    private CheckpointTopicVerifier CreateVerifier(string topicName, RecordingLogger<CheckpointTopicVerifier> logger) =>
        new(
            fixture.AdminClient,
            fixture.BuildOptions<KafkaOptions>(
                KafkaOptions.ConfigurationSection,
                (KafkaOptions.ConfigurationSection + ":CheckpointTopic", topicName)),
            logger);

    private async Task WaitUntilTopicVisibleAsync(string topicName)
    {
        for (var attempt = 0; attempt < 20; attempt++)
        {
            var metadata = fixture.AdminClient.GetMetadata(TimeSpan.FromSeconds(5));
            if (metadata.Topics.Any(topic => topic.Topic == topicName))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }

        throw new TimeoutException($"Topic '{topicName}' не появился в метадате Kafka за 10 с после создания.");
    }
}
