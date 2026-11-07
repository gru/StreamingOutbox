namespace KafkaValidator.Contracts;

/// <summary>
/// Echo фактической конфигурации в снапшоте состояния.
/// </summary>
/// <param name="Brokers">Список брокеров.</param>
/// <param name="Topic">Topic консюмера.</param>
/// <param name="GroupId">Consumer-группа.</param>
/// <param name="WriterBaseUrl">Базовый URL Writer (fetchFromWriter).</param>
public sealed record ConfigEcho(string Brokers, string Topic, string GroupId, string WriterBaseUrl);
