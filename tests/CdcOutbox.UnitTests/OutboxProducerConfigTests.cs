using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using Confluent.Kafka;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class OutboxProducerConfigTests
{
    [Fact]
    public void Build_MapsProducerFields()
    {
        var kafka = new KafkaOptions
        {
            BootstrapServers = "kafka1:9092,kafka2:9092",
            EnableIdempotence = true,
            BatchNumMessages = 100,
            LingerMs = 7,
            DeliveryTimeoutMs = 30000,
            MessageSendMaxRetries = 12,
            MessageMaxBytes = 12345,
        };

        var config = OutboxProducerConfig.Build(kafka, "outbox_slot");

        config.BootstrapServers.ShouldBe("kafka1:9092,kafka2:9092");
        config.Acks.ShouldBe(Acks.All);
        config.EnableIdempotence.ShouldBe(true);
        config.BatchNumMessages.ShouldBe(100);
        config.LingerMs.ShouldBe(7);
        config.MessageTimeoutMs.ShouldBe(30000);
        config.MessageSendMaxRetries.ShouldBe(12);
        config.MessageMaxBytes.ShouldBe(12345);
        config.SecurityProtocol.ShouldBeNull();
    }

    [Fact]
    public void Build_DerivesTransactionalIdFromSlot_AndTransactionTimeoutFromDelivery()
    {
        var kafka = new KafkaOptions { BootstrapServers = "kafka:9092", DeliveryTimeoutMs = 120000 };

        var config = OutboxProducerConfig.Build(kafka, "outbox_slot");

        config.TransactionalId.ShouldBe("cdc-outbox-outbox_slot");
        // транзакция живёт дольше доставки батча: DeliveryTimeoutMs + запас на commit
        config.TransactionTimeoutMs.ShouldBe(180000);
        OutboxProducerConfig.BuildTransactionalId("other_slot").ShouldBe("cdc-outbox-other_slot");
    }

    [Fact]
    public void BuildControlConsumerConfig_MapsArbitrationSettings()
    {
        var kafka = new KafkaOptions
        {
            BootstrapServers = "kafka:9092",
            ControlGroupId = "cdc-outbox-pair",
            ControlSessionTimeoutSeconds = 15,
        };

        var config = OutboxProducerConfig.BuildControlConsumerConfig(kafka);

        config.GroupId.ShouldBe("cdc-outbox-pair");
        config.EnableAutoCommit.ShouldBe(false);
        config.PartitionAssignmentStrategy.ShouldBe(PartitionAssignmentStrategy.CooperativeSticky);
        config.SessionTimeoutMs.ShouldBe(15000);
        // правило «3 heartbeat на сессию»
        config.HeartbeatIntervalMs.ShouldBe(5000);
        config.AutoOffsetReset.ShouldBe(AutoOffsetReset.Latest);
        config.SecurityProtocol.ShouldBeNull();
    }

    [Fact]
    public void BuildCheckpointConsumerConfig_ReadsCommitted()
    {
        var config = OutboxProducerConfig.BuildCheckpointConsumerConfig(
            new KafkaOptions { BootstrapServers = "kafka:9092" });

        config.GroupId.ShouldBe("cdc-outbox-checkpoint-reader");
        config.EnableAutoCommit.ShouldBe(false);
        config.IsolationLevel.ShouldBe(IsolationLevel.ReadCommitted);
    }

    [Fact]
    public void ApplySecurity_Plaintext_DoesNotConfigureSsl()
    {
        var config = new ClientConfig();

        OutboxProducerConfig.ApplySecurity(config, new KafkaOptions { BootstrapServers = "kafka:9092" });

        config.SecurityProtocol.ShouldBeNull();
        config.SslCertificateLocation.ShouldBeNull();
    }

    [Fact]
    public void ApplySecurity_SslFiles_SetsLocations()
    {
        var kafka = new KafkaOptions
        {
            BootstrapServers = "kafka:9092",
            SecurityProtocol = "ssl",
            SslCaLocation = "/certs/ca.pem",
            SslCertificateLocation = "/certs/client.pem",
            SslKeyLocation = "/certs/client.key",
            SslKeyPassword = "secret",
        };
        var config = new ClientConfig();

        OutboxProducerConfig.ApplySecurity(config, kafka);

        config.SecurityProtocol.ShouldBe(SecurityProtocol.Ssl);
        config.SslCaLocation.ShouldBe("/certs/ca.pem");
        config.SslCertificateLocation.ShouldBe("/certs/client.pem");
        config.SslKeyLocation.ShouldBe("/certs/client.key");
        config.SslKeyPassword.ShouldBe("secret");
    }

    [Fact]
    public void ApplySecurity_SslThumbprint_SetsPemStrings()
    {
        using var certificate = InstallSelfSignedCertificate();
        try
        {
            var kafka = new KafkaOptions
            {
                BootstrapServers = "kafka:9092",
                SecurityProtocol = "ssl",
                SslCertificateThumbprint = certificate.Thumbprint,
            };
            var config = new ClientConfig();

            OutboxProducerConfig.ApplySecurity(config, kafka);

            config.SecurityProtocol.ShouldBe(SecurityProtocol.Ssl);
            config.SslCertificatePem.ShouldContain("BEGIN CERTIFICATE");
            config.SslKeyPem.ShouldContain("BEGIN PRIVATE KEY");
            config.SslCertificateLocation.ShouldBeNull();
        }
        finally
        {
            RemoveCertificate(certificate);
        }
    }

    [Fact]
    public void ResolveStoreCertificate_InstalledCertificate_ExportsPem()
    {
        using var certificate = InstallSelfSignedCertificate();
        try
        {
            var (certificatePem, keyPem) = OutboxProducerConfig.ResolveStoreCertificate(
                certificate.Thumbprint,
                "My",
                "CurrentUser");

            certificatePem.ShouldBe(certificate.ExportCertificatePem());
            keyPem.ShouldContain("BEGIN PRIVATE KEY");
        }
        finally
        {
            RemoveCertificate(certificate);
        }
    }

    [Fact]
    public void ResolveStoreCertificate_MissingThumbprint_ThrowsWithThumbprint()
    {
        var thumbprint = "0123456789ABCDEF0123456789ABCDEF01234567";

        var error = Should.Throw<InvalidOperationException>(
            () => OutboxProducerConfig.ResolveStoreCertificate(thumbprint, "My", "CurrentUser"));

        error.Message.ShouldContain(thumbprint);
        error.Message.ShouldContain("не найден");
    }

    [Fact]
    public void ResolveStoreCertificate_UnknownLocation_ThrowsNamingSetting()
    {
        var error = Should.Throw<InvalidOperationException>(
            () => OutboxProducerConfig.ResolveStoreCertificate("0123", "My", "Somewhere"));

        error.Message.ShouldContain("SslStoreLocation");
    }

    private static X509Certificate2 InstallSelfSignedCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=cdc-outbox-unit",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
        store.Open(OpenFlags.ReadWrite);
        store.Add(certificate);
        return certificate;
    }

    private static void RemoveCertificate(X509Certificate2 certificate)
    {
        using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
        store.Open(OpenFlags.ReadWrite);
        store.Remove(certificate);
    }
}
