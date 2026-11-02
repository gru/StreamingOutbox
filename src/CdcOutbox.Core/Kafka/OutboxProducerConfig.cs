using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CdcOutbox.Core.Configuration;
using Confluent.Kafka;

namespace CdcOutbox.Core.Kafka;

/// <summary>
/// Конфигурации Kafka-клиентов сервиса: продюсер событий, административный
/// клиент стартовых проверок и консюмер чтения checkpoint применяют
/// одну и ту же конфигурацию безопасности (mTLS по клиентскому
/// сертификату — из файлов или из хранилища сертификатов).
/// </summary>
public static class OutboxProducerConfig
{
    /// <summary>
    /// Собрать конфигурацию продюсера событий.
    /// </summary>
    /// <param name="kafka">Параметры Kafka.</param>
    /// <returns>Конфигурация продюсера.</returns>
    public static ProducerConfig Build(KafkaOptions kafka)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            Acks = ParseAcks(kafka.Acks),
            EnableIdempotence = kafka.EnableIdempotence,
            BatchNumMessages = kafka.BatchNumMessages,
            LingerMs = kafka.LingerMs,
            MessageTimeoutMs = kafka.DeliveryTimeoutMs,
            MessageSendMaxRetries = kafka.MessageSendMaxRetries,
            MessageMaxBytes = kafka.MessageMaxBytes,
        };
        ApplySecurity(config, kafka);
        return config;
    }

    /// <summary>
    /// Собрать конфигурацию административного клиента (та же безопасность, что у продюсера).
    /// </summary>
    /// <param name="kafka">Параметры Kafka.</param>
    /// <returns>Конфигурация административного клиента.</returns>
    public static AdminClientConfig BuildAdminConfig(KafkaOptions kafka)
    {
        var config = new AdminClientConfig { BootstrapServers = kafka.BootstrapServers };
        ApplySecurity(config, kafka);
        return config;
    }

    /// <summary>
    /// Собрать конфигурацию консюмера чтения checkpoint (та же безопасность,
    /// что у продюсера).
    /// </summary>
    /// <param name="kafka">Параметры Kafka.</param>
    /// <returns>Конфигурация консюмера.</returns>
    public static ConsumerConfig BuildCheckpointConsumerConfig(KafkaOptions kafka)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            // группа фиктивна: клиент требует group.id даже при явном назначении
            // партиции; Subscribe и коммиты офсетов не используются —
            // групповой координации нет
            GroupId = "cdc-outbox-checkpoint-reader",
            EnableAutoCommit = false,
        };
        ApplySecurity(config, kafka);
        return config;
    }

    /// <summary>
    /// Применить параметры безопасности к конфигурации клиента Kafka.
    /// </summary>
    /// <param name="config">Конфигурация клиента.</param>
    /// <param name="kafka">Параметры Kafka.</param>
    public static void ApplySecurity(ClientConfig config, KafkaOptions kafka)
    {
        if (!kafka.SecurityProtocol.Trim().Equals("ssl", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        config.SecurityProtocol = SecurityProtocol.Ssl;
        if (!string.IsNullOrWhiteSpace(kafka.SslCaLocation))
        {
            config.SslCaLocation = kafka.SslCaLocation;
        }

        if (!string.IsNullOrWhiteSpace(kafka.SslCertificateThumbprint))
        {
            var (certificatePem, keyPem) = ResolveStoreCertificate(
                kafka.SslCertificateThumbprint,
                kafka.SslStoreName,
                kafka.SslStoreLocation);
            config.SslCertificatePem = certificatePem;
            config.SslKeyPem = keyPem;
            return;
        }

        config.SslCertificateLocation = kafka.SslCertificateLocation;
        config.SslKeyLocation = kafka.SslKeyLocation;
        if (!string.IsNullOrWhiteSpace(kafka.SslKeyPassword))
        {
            config.SslKeyPassword = kafka.SslKeyPassword;
        }
    }

    /// <summary>
    /// Разобрать режим подтверждения записи.
    /// </summary>
    /// <param name="value">Значение настройки Acks.</param>
    /// <returns>Режим подтверждения.</returns>
    public static Acks ParseAcks(string value)
    {
        var normalized = value.Trim().ToLowerInvariant();
        return normalized switch
        {
            "all" or "-1" => Acks.All,
            "1" => Acks.Leader,
            "0" or "none" => Acks.None,
            _ => throw new InvalidOperationException(
                $"CdcOutbox:Kafka:Acks '{value}' не распознан: допустимы all, 1, 0."),
        };
    }

    /// <summary>
    /// Найти клиентский сертификат в хранилище по thumbprint и экспортировать в PEM.
    /// </summary>
    /// <param name="thumbprint">SHA-1 thumbprint сертификата.</param>
    /// <param name="storeName">Имя хранилища сертификатов.</param>
    /// <param name="storeLocation">Расположение хранилища: CurrentUser или LocalMachine.</param>
    /// <returns>PEM сертификата и PEM приватного ключа.</returns>
    public static (string CertificatePem, string KeyPem) ResolveStoreCertificate(
        string thumbprint,
        string storeName,
        string storeLocation)
    {
        var location = storeLocation.Trim().ToLowerInvariant() switch
        {
            "currentuser" => StoreLocation.CurrentUser,
            "localmachine" => StoreLocation.LocalMachine,
            _ => throw new InvalidOperationException(
                $"CdcOutbox:Kafka:SslStoreLocation '{storeLocation}' не распознано: допустимы CurrentUser, LocalMachine."),
        };
        var normalized = thumbprint.Replace(" ", string.Empty, StringComparison.Ordinal);
        using var store = new X509Store(storeName, location);
        store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);
        var found = store.Certificates.Find(X509FindType.FindByThumbprint, normalized, validOnly: false);
        if (found.Count == 0)
        {
            throw new InvalidOperationException(
                $"Клиентский сертификат с thumbprint {normalized} не найден в хранилище {storeName}/{location} — проверьте установку сертификата.");
        }

        var certificate = found[0];
        if (!certificate.HasPrivateKey)
        {
            throw new InvalidOperationException(
                $"Сертификат {normalized} из хранилища {storeName}/{location} не содержит приватного ключа.");
        }

        string keyPem;
        try
        {
            keyPem = ExportPrivateKeyPem(certificate);
        }
        catch (Exception exception) when (exception is CryptographicException or NotSupportedException)
        {
            throw new InvalidOperationException(
                $"Приватный ключ сертификата {normalized} не экспортируется: ключ из хранилища передаётся брокеру PEM-строкой, неэкспортируемые ключи (токен/HSM) не поддерживаются.",
                exception);
        }

        return (certificate.ExportCertificatePem(), keyPem);
    }

    private static string ExportPrivateKeyPem(X509Certificate2 certificate)
    {
        using var key = certificate.GetRSAPrivateKey() ?? (AsymmetricAlgorithm?)certificate.GetECDsaPrivateKey();
        return key switch
        {
            RSA rsa => rsa.ExportPkcs8PrivateKeyPem(),
            ECDsa ecdsa => ecdsa.ExportPkcs8PrivateKeyPem(),
            _ => throw new NotSupportedException("Тип приватного ключа не поддерживается."),
        };
    }
}
