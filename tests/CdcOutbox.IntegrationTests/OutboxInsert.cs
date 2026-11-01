namespace CdcOutbox.IntegrationTests;

public sealed record OutboxInsert(
    string Topic,
    byte[] Key,
    byte[] Body,
    string Headers = "{}",
    int? Partition = null,
    string? ProducerKey = null,
    DateTime? CreatedAt = null);
