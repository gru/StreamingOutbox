using CdcOutbox.Core.Configuration;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Kafka;

/// <summary>
/// Проверка наличия checkpoint-topic в кластере Kafka по метадате кластера;
/// имя topic — <see cref="KafkaOptions.CheckpointTopic"/>.
/// </summary>
public sealed class CheckpointTopicVerifier
{
    private static readonly TimeSpan MetadataTimeout = TimeSpan.FromSeconds(10);

    private readonly IAdminClient _adminClient;
    private readonly KafkaOptions _kafka;
    private readonly ILogger<CheckpointTopicVerifier> _logger;

    /// <summary>
    /// Создать проверяльщик checkpoint-topic.
    /// </summary>
    /// <param name="adminClient">Клиент администрирования Kafka.</param>
    /// <param name="kafka">Параметры Kafka (bootstrap-серверы, имя checkpoint-topic).</param>
    /// <param name="logger">Журнал.</param>
    public CheckpointTopicVerifier(
        IAdminClient adminClient,
        IOptions<KafkaOptions> kafka,
        ILogger<CheckpointTopicVerifier> logger)
    {
        _adminClient = adminClient;
        _kafka = kafka.Value;
        _logger = logger;
    }

    /// <summary>
    /// Запросить метадату кластера и убедиться, что checkpoint-topic существует;
    /// отсутствие topic или недоступность кластера — исключение.
    /// </summary>
    public void Verify()
    {
        Metadata metadata;
        try
        {
            metadata = _adminClient.GetMetadata(MetadataTimeout);
        }
        catch (KafkaException ex)
        {
            throw new InvalidOperationException(
                $"Метадата Kafka недоступна — не удалось проверить topic '{_kafka.CheckpointTopic}': {ex.Message}", ex);
        }

        var issue = FindTopicIssue(metadata, _kafka.CheckpointTopic);
        if (issue is not null)
        {
            throw new InvalidOperationException(issue);
        }

        _logger.LogInformation("Topic '{Topic}' существует (checkpoint-стор).", _kafka.CheckpointTopic);
    }

    /// <summary>
    /// Проверить метадату кластера на наличие topic.
    /// </summary>
    /// <param name="metadata">Метадата кластера Kafka.</param>
    /// <param name="topicName">Имя topic.</param>
    /// <returns>Описание проблемы; null, если topic существует.</returns>
    public static string? FindTopicIssue(Metadata metadata, string topicName)
    {
        if (metadata.Topics.All(topic => topic.Topic != topicName))
        {
            return $"Topic '{topicName}' отсутствует в Kafka: сервис не создаёт topic'и — создай его и перезапусти сервис.";
        }

        return null;
    }
}
