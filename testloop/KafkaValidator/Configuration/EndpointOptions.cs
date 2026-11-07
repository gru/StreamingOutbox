namespace KafkaValidator.Configuration;

/// <summary>
/// Эндпоинт внешнего сервиса.
/// </summary>
public sealed class EndpointOptions
{
    /// <summary>
    /// Базовый URL (без хвостового слэша).
    /// </summary>
    public string BaseUrl { get; set; } = "";
}
