namespace KafkaValidator.Configuration;

/// <summary>
/// Конфигурация KafkaValidator (секция "KafkaValidator", env-префикс
/// KAFKAVALIDATOR__ — одним словом: одиночные подчёркивания не являются
/// разделителями для провайдера EnvironmentVariables).
///
/// Ключевая семантика консюмера (группа + Earliest + без сохранения оффсетов)
/// зашита в коде — это не опция: полная перемотка topic при каждом старте
/// гарантирует полноту валидации после рестарта (иначе закоммиченные оффсеты
/// «съедят» историю и валидатор ложно сообщит о потерях).
/// </summary>
public sealed class KafkaValidatorOptions
{
    /// <summary>
    /// Имя секции конфигурации в appsettings / env (KAFKAVALIDATOR__...).
    /// </summary>
    public const string SectionName = "KafkaValidator";

    /// <summary>
    /// Подключение к Kafka: брокеры, topic, параметры консюмера.
    /// </summary>
    public KafkaOptions Kafka { get; set; } = new();

    /// <summary>
    /// Откуда валидатор берёт ожидаемый набор id (fetchFromWriter).
    /// </summary>
    public EndpointOptions Writer { get; set; } = new() { BaseUrl = "http://writer:8080" };
}
