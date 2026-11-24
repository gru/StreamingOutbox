namespace CdcOutbox.Core.Partitions;

/// <summary>
/// Результат проверки и предсоздания будущих часовых партиций.
/// </summary>
/// <param name="CreatedCount">Количество вновь созданных партиций за этот вызов.</param>
/// <param name="FuturePartitionsCount">Суммарное количество готовых будущих часовых партиций после проверки.</param>
public sealed record PartitionEnsureResult(int CreatedCount, int FuturePartitionsCount);
