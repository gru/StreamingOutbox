using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CdcOutbox.Core.Configuration;
using Confluent.Kafka;

namespace CdcOutbox.Core.Kafka;

/// <summary>
/// Конфигурации Kafka-клиентов сервиса (продюсер, AdminClient, консюмеры)
/// с общей конфигурацией безопасности (mTLS).
/// </summary>
public static class OutboxProducerConfig
{
    /// <summary>
    /// Бюджет операций транзакций (commit/abort/init): с запасом к обычным
    /// единицам секунд, но меньше TransactionTimeoutMs.
    /// </summary>
    internal static readonly TimeSpan TransactionOperationTimeout = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Запас transaction.timeout.ms к DeliveryTimeoutMs: медленная доставка
    /// (до MessageTimeoutMs) плюс commit.
    /// </summary>
    private const int TransactionTimeoutMarginMs = 60_000;

    /// <summary>
    /// Постоянный transactional.id слота (основа fencing).
    /// </summary>
    /// <param name="slotName">Имя replication slot.</param>
    /// <returns>transactional.id.</returns>
    public static string BuildTransactionalId(string slotName) => $"cdc-outbox-{slotName}";

    /// <summary>
    /// Контрольная группа арбитража Active-Passive слота: назначение партиции
    /// группы = лидерство. Одинакова у всех экземпляров пары слота и не
    /// пересекается с парами других слотов общего checkpoint-топика; офсеты
    /// группы не читаются и не коммитятся.
    /// </summary>
    /// <param name="slotName">Имя replication slot.</param>
    /// <returns>Имя контрольной consumer group.</returns>
    public static string BuildControlGroupId(string slotName) => $"cdc-outbox-{slotName}";

    /// <summary>
    /// Собрать конфигурацию транзакционного продюсера.
    /// </summary>
    /// <param name="kafka">Параметры Kafka.</param>
    /// <param name="slotName">Имя replication slot — основа transactional.id.</param>
    /// <returns>Конфигурация продюсера.</returns>
    public static ProducerConfig Build(KafkaOptions kafka, string slotName)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            // транзакции требуют подтверждения всеми синхронными репликами —
            // значение зашито, настройкой не является
            Acks = Acks.All,
            EnableIdempotence = kafka.EnableIdempotence,
            TransactionalId = BuildTransactionalId(slotName),
            // потолок окружения — transaction.max.timeout.ms (контракт окружения)
            TransactionTimeoutMs = kafka.DeliveryTimeoutMs + TransactionTimeoutMarginMs,
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
    /// Собрать конфигурацию консюмера чтения checkpoint (read_committed).
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
            IsolationLevel = IsolationLevel.ReadCommitted,
        };
        ApplySecurity(config, kafka);
        return config;
    }

    /// <summary>
    /// Собрать конфигурацию контрольного консюмера арбитража Active-Passive.
    /// </summary>
    /// <param name="kafka">Параметры Kafka.</param>
    /// <param name="slotName">Имя replication slot — основа контрольной группы.</param>
    /// <returns>Конфигурация консюмера.</returns>
    public static ConsumerConfig BuildControlConsumerConfig(KafkaOptions kafka, string slotName)
    {
        var sessionTimeoutMs = kafka.ControlSessionTimeoutSeconds * 1000;
        var config = new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = BuildControlGroupId(slotName),
            EnableAutoCommit = false,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            // правило «3 heartbeat на сессию» при любом значении session
            SessionTimeoutMs = sessionTimeoutMs,
            HeartbeatIntervalMs = sessionTimeoutMs / 3,
            // записи контрольного топика не используются — история не нужна
            AutoOffsetReset = AutoOffsetReset.Latest,
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
