using System.ComponentModel.DataAnnotations;

namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Параметры Kafka: кластер, продюсер событий и checkpoint-стор.
/// </summary>
public sealed class KafkaOptions
{
    /// <summary>
    /// Путь секции конфигурации.
    /// </summary>
    public const string ConfigurationSection = "CdcOutbox:Kafka";

    /// <summary>
    /// Список bootstrap-серверов Kafka.
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Kafka:BootstrapServers не задан.")]
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Идемпотентный продюсер (ProducerConfig EnableIdempotence).
    /// </summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>
    /// Батчинг продюсера: сообщений на батч (ProducerConfig BatchNumMessages).
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:BatchNumMessages должен быть положительным.")]
    public int BatchNumMessages { get; set; } = 10000;

    /// <summary>
    /// Задержка накопления батча, мс (ProducerConfig LingerMs).
    /// </summary>
    [Range(0, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:LingerMs не может быть отрицательным.")]
    public int LingerMs { get; set; } = 5;

    /// <summary>
    /// Таймаут доставки сообщения, включая ретраи, мс (ProducerConfig MessageTimeoutMs).
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:DeliveryTimeoutMs должен быть положительным.")]
    public int DeliveryTimeoutMs { get; set; } = 120000;

    /// <summary>
    /// Лимит ретраев отправки (ProducerConfig MessageSendMaxRetries).
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:MessageSendMaxRetries должен быть положительным.")]
    public int MessageSendMaxRetries { get; set; } = int.MaxValue;

    /// <summary>
    /// Максимальный размер сообщения, байт (ProducerConfig MessageMaxBytes).
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:MessageMaxBytes должен быть положительным.")]
    public int MessageMaxBytes { get; set; } = 1000000;

    /// <summary>
    /// Служебный topic checkpoint-стора (log compaction).
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Kafka:CheckpointTopic не задан.")]
    public string CheckpointTopic { get; set; } = "cdc-checkpoints";

    /// <summary>
    /// Таймаут сессии контрольной группы, с (ConsumerConfig SessionTimeoutMs):
    /// время обнаружения внезапной смерти лидера координатором группы. Должен
    /// попадать в границы параметров группы окружения
    /// [group.min.session.timeout.ms, group.max.session.timeout.ms].
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:ControlSessionTimeoutSeconds должен быть положительным.")]
    public int ControlSessionTimeoutSeconds { get; set; } = 15;

    /// <summary>
    /// Бюджет чтения последнего checkpoint при старте, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:CheckpointReadTimeoutSeconds должен быть положительным.")]
    public int CheckpointReadTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Такт опроса консюмера при чтении checkpoint, с.
    /// </summary>
    [Range(1, int.MaxValue, ErrorMessage = "CdcOutbox:Kafka:CheckpointPollTimeoutSeconds должен быть положительным.")]
    public int CheckpointPollTimeoutSeconds { get; set; } = 2;

    /// <summary>
    /// Протокол безопасности Kafka: plaintext или ssl (mTLS — авторизация клиентским сертификатом).
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Kafka:SecurityProtocol не задан.")]
    public string SecurityProtocol { get; set; } = "plaintext";

    /// <summary>
    /// PEM-файл CA для проверки брокеров (режим ssl; на Windows допустимы root/probe — системное хранилище).
    /// </summary>
    public string? SslCaLocation { get; set; }

    /// <summary>
    /// PEM-файл клиентского сертификата (режим ssl, источник «из файлов»).
    /// </summary>
    public string? SslCertificateLocation { get; set; }

    /// <summary>
    /// PEM-файл приватного ключа (задаётся вместе с сертификатом).
    /// </summary>
    public string? SslKeyLocation { get; set; }

    /// <summary>
    /// Пароль приватного ключа, если зашифрован (источник «из файлов»).
    /// </summary>
    public string? SslKeyPassword { get; set; }

    /// <summary>
    /// SHA-1 thumbprint клиентского сертификата в хранилище сертификатов (режим ssl, источник «из хранилища»).
    /// </summary>
    public string? SslCertificateThumbprint { get; set; }

    /// <summary>
    /// Имя хранилища сертификатов (источник «из хранилища»).
    /// </summary>
    public string SslStoreName { get; set; } = "My";

    /// <summary>
    /// Расположение хранилища сертификатов: CurrentUser или LocalMachine.
    /// </summary>
    public string SslStoreLocation { get; set; } = "CurrentUser";

    /// <summary>
    /// Проверить согласованность параметров безопасности Kafka.
    /// </summary>
    /// <param name="kafka">Параметры Kafka.</param>
    /// <returns>Текст проблемы или null, если конфигурация корректна.</returns>
    public static string? FindSecurityIssue(KafkaOptions kafka)
    {
        var hasCa = !string.IsNullOrWhiteSpace(kafka.SslCaLocation);
        var hasCertificateFile = !string.IsNullOrWhiteSpace(kafka.SslCertificateLocation);
        var hasKeyFile = !string.IsNullOrWhiteSpace(kafka.SslKeyLocation);
        var hasPassword = !string.IsNullOrWhiteSpace(kafka.SslKeyPassword);
        var hasThumbprint = !string.IsNullOrWhiteSpace(kafka.SslCertificateThumbprint);
        var protocol = kafka.SecurityProtocol.Trim().ToLowerInvariant();
        if (protocol == "plaintext")
        {
            return hasCa || hasCertificateFile || hasKeyFile || hasPassword || hasThumbprint
                ? "CdcOutbox:Kafka:SecurityProtocol=plaintext, но заданы параметры SSL — укажите SecurityProtocol=ssl или уберите SSL-параметры."
                : null;
        }

        if (protocol != "ssl")
        {
            return "CdcOutbox:Kafka:SecurityProtocol должен быть plaintext или ssl.";
        }

        if (hasCertificateFile && hasThumbprint)
        {
            return "CdcOutbox:Kafka: заданы оба источника клиентского сертификата — файлы (SslCertificateLocation/SslKeyLocation) и хранилище (SslCertificateThumbprint); нужен ровно один.";
        }

        if (!hasCertificateFile && !hasThumbprint)
        {
            return "CdcOutbox:Kafka:SecurityProtocol=ssl требует клиентский сертификат — файлы (SslCertificateLocation+SslKeyLocation) или хранилище (SslCertificateThumbprint).";
        }

        if (hasCertificateFile != hasKeyFile)
        {
            return "CdcOutbox:Kafka: файловый источник сертификата неполный — SslCertificateLocation и SslKeyLocation задаются вместе.";
        }

        if (hasThumbprint && hasPassword)
        {
            return "CdcOutbox:Kafka:SslKeyPassword применим только к файловому ключу — источник «из хранилища» пароля не использует.";
        }

        return null;
    }

}
