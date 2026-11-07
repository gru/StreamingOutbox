using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Processing;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Kafka;

/// <summary>
/// Проверяет checkpoint-топик: существует и имеет ровно одну партицию.
/// </summary>
public sealed class CheckpointTopicVerifier(
    IAdminClient adminClient,
    IOptions<KafkaOptions> kafka,
    ILogger<CheckpointTopicVerifier> logger)
{
    private static readonly TimeSpan MetadataTimeout = TimeSpan.FromSeconds(10);

    private readonly KafkaOptions _kafka = kafka.Value;

    /// <summary>
    /// Проверяет checkpoint-топик по метадате кластера.
    /// </summary>
    public void Verify()
    {
        Metadata metadata;
        try
        {
            metadata = adminClient.GetMetadata(MetadataTimeout);
        }
        catch (KafkaException ex)
        {
            throw new InvalidOperationException(
                $"Метадата Kafka недоступна — не удалось проверить topic '{_kafka.CheckpointTopic}': {ex.Message}", ex);
        }

        var issue = FindTopicIssue(metadata, _kafka.CheckpointTopic);
        if (issue is not null)
        {
            throw new StartupCheckException(issue);
        }

        logger.LogInformation("Topic '{Topic}' существует (checkpoint-стор).", _kafka.CheckpointTopic);
    }

    /// <summary>
    /// Возвращает описание проблемы несоответствия checkpoint-топика контракту:
    /// отсутствует или партиций не одна.
    /// </summary>
    /// <param name="metadata">Метадата кластера Kafka.</param>
    /// <param name="topicName">Имя topic.</param>
    /// <returns>Описание проблемы; null, если топик соответствует контракту.</returns>
    public static string? FindTopicIssue(Metadata metadata, string topicName)
    {
        var topic = metadata.Topics.FirstOrDefault(topic => topic.Topic == topicName);
        if (topic is null)
        {
            return $"Topic '{topicName}' отсутствует в Kafka: сервис не создаёт topic'и — создай его и перезапусти сервис.";
        }

        if (topic.Partitions.Count != 1)
        {
            return $"Topic '{topicName}' имеет {topic.Partitions.Count} партиций, ожидается ровно 1: " +
                   "лидером Active-Passive становится владелец единственной партиции контрольной группы — " +
                   "при другом количестве партиций резервирование не работает.";
        }

        return null;
    }
}
