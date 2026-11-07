using System.ComponentModel.DataAnnotations;
using CdcOutbox.Core.Configuration;
using Microsoft.Extensions.Configuration;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class OptionsTests
{
    [Fact]
    public void Defaults_MatchConfigurationTable()
    {
        var root = new CdcOutboxOptions();
        var postgres = new PostgresOptions();
        var replication = new ReplicationOptions();
        var storage = new OutboxStorageOptions();
        var kafka = new KafkaOptions();
        var processor = new ProcessorOptions();

        root.RecoveryMode.ShouldBeFalse();
        postgres.ConnectionString.ShouldBe(string.Empty);
        replication.SlotName.ShouldBe(string.Empty);
        replication.PublicationName.ShouldBe(string.Empty);
        replication.Binary.ShouldBeFalse();
        storage.Schema.ShouldBe("outbox");
        storage.Table.ShouldBe("messages");
        kafka.BootstrapServers.ShouldBe(string.Empty);
        kafka.EnableIdempotence.ShouldBeTrue();
        kafka.BatchNumMessages.ShouldBe(10000);
        kafka.LingerMs.ShouldBe(5);
        kafka.DeliveryTimeoutMs.ShouldBe(120000);
        kafka.MessageSendMaxRetries.ShouldBe(int.MaxValue);
        kafka.MessageMaxBytes.ShouldBe(1000000);
        kafka.CheckpointTopic.ShouldBe("cdc-checkpoints");
        kafka.ControlSessionTimeoutSeconds.ShouldBe(15);
        kafka.CheckpointReadTimeoutSeconds.ShouldBe(30);
        kafka.CheckpointPollTimeoutSeconds.ShouldBe(2);
        kafka.SecurityProtocol.ShouldBe("plaintext");
        kafka.SslCaLocation.ShouldBeNull();
        kafka.SslCertificateLocation.ShouldBeNull();
        kafka.SslKeyLocation.ShouldBeNull();
        kafka.SslKeyPassword.ShouldBeNull();
        kafka.SslCertificateThumbprint.ShouldBeNull();
        kafka.SslStoreName.ShouldBe("My");
        kafka.SslStoreLocation.ShouldBe("CurrentUser");
        processor.QueueCapacity.ShouldBe(2048);
        processor.BatchSize.ShouldBe(500);
        processor.CheckpointIntervalSeconds.ShouldBe(5);
        processor.IdleTimeoutSeconds.ShouldBe(30);
        processor.RetryDelaySeconds.ShouldBe(5);
        processor.MaxRetryDelaySeconds.ShouldBe(60);
        processor.BackoffResetSessionSeconds.ShouldBe(30);
        processor.FinalCheckpointWaitSeconds.ShouldBe(15);
        processor.ShutdownTimeoutSeconds.ShouldBe(30);
    }

    [Fact]
    public void ConfigurationSections_AreCdcOutboxSections()
    {
        CdcOutboxOptions.ConfigurationSection.ShouldBe("CdcOutbox");
        PostgresOptions.ConfigurationSection.ShouldBe("CdcOutbox:Postgres");
        ReplicationOptions.ConfigurationSection.ShouldBe("CdcOutbox:Replication");
        OutboxStorageOptions.ConfigurationSection.ShouldBe("CdcOutbox:Storage");
        KafkaOptions.ConfigurationSection.ShouldBe("CdcOutbox:Kafka");
        ProcessorOptions.ConfigurationSection.ShouldBe("CdcOutbox:Processor");
    }

    [Fact]
    public void Defaults_WithoutRequiredEndpoints_FailDataAnnotationValidation()
    {
        var postgres = Validate(new PostgresOptions());
        postgres.ShouldHaveSingleItem().ErrorMessage!.ShouldContain("ConnectionString");
        var replication = Validate(new ReplicationOptions());
        replication.Count.ShouldBe(2);
        replication.ShouldContain(r => r.ErrorMessage!.Contains("SlotName"));
        replication.ShouldContain(r => r.ErrorMessage!.Contains("PublicationName"));
        var kafka = Validate(new KafkaOptions());
        kafka.ShouldHaveSingleItem().ErrorMessage!.ShouldContain("BootstrapServers");
    }

    [Fact]
    public void FullyConfiguredOptions_PassDataAnnotationValidation()
    {
        var postgres = new PostgresOptions { ConnectionString = "Host=pg;Database=d;Username=u" };
        var replication = new ReplicationOptions { SlotName = "outbox_slot", PublicationName = "outbox_publication" };
        var storage = new OutboxStorageOptions();
        var kafka = new KafkaOptions { BootstrapServers = "kafka1:9092,kafka2:9092,kafka3:9092" };
        var processor = new ProcessorOptions();

        Validate(postgres).ShouldBeEmpty();
        Validate(replication).ShouldBeEmpty();
        Validate(storage).ShouldBeEmpty();
        Validate(kafka).ShouldBeEmpty();
        Validate(processor).ShouldBeEmpty();
    }

    [Fact]
    public void EnvironmentVariables_BindToOptionsSections()
    {
        Environment.SetEnvironmentVariable("CDCOUTBOX__POSTGRES__CONNECTIONSTRING", "Host=pg1;Database=db;Username=usr");
        Environment.SetEnvironmentVariable("CDCOUTBOX__PROCESSOR__BATCHSIZE", "77");
        Environment.SetEnvironmentVariable("CDCOUTBOX__RECOVERYMODE", "true");
        try
        {
            var configuration = new ConfigurationManager();
            configuration.AddEnvironmentVariables();

            var postgres = configuration.GetSection("CdcOutbox:Postgres").Get<PostgresOptions>();
            var processor = configuration.GetSection("CdcOutbox:Processor").Get<ProcessorOptions>();
            var root = configuration.GetSection("CdcOutbox").Get<CdcOutboxOptions>();

            postgres.ShouldNotBeNull();
            postgres.ConnectionString.ShouldBe("Host=pg1;Database=db;Username=usr");
            processor.ShouldNotBeNull();
            processor.BatchSize.ShouldBe(77);
            root.ShouldNotBeNull();
            root.RecoveryMode.ShouldBeTrue();
        }
        finally
        {
            Environment.SetEnvironmentVariable("CDCOUTBOX__POSTGRES__CONNECTIONSTRING", null);
            Environment.SetEnvironmentVariable("CDCOUTBOX__PROCESSOR__BATCHSIZE", null);
            Environment.SetEnvironmentVariable("CDCOUTBOX__RECOVERYMODE", null);
        }
    }

    [Fact]
    public void Postgres_EmptyConnectionString_FailsRequired()
    {
        var error = Validate(new PostgresOptions { ConnectionString = "" }).ShouldHaveSingleItem().ErrorMessage;

        error.ShouldNotBeNull();
        error.ShouldContain("ConnectionString");
    }

    [Fact]
    public void Required_Fields_FailValidation()
    {
        RequiredError(new ReplicationOptions { SlotName = "", PublicationName = "outbox_publication" }).ShouldContain("SlotName");
        RequiredError(new ReplicationOptions { SlotName = "outbox_slot", PublicationName = "" }).ShouldContain("PublicationName");
        RequiredError(new OutboxStorageOptions { Schema = "" }).ShouldContain("Schema");
        RequiredError(new OutboxStorageOptions { Table = "" }).ShouldContain("Table");
        RequiredError(new KafkaOptions { BootstrapServers = "" }).ShouldContain("BootstrapServers");
        RequiredError(new KafkaOptions { BootstrapServers = "kafka:9092", CheckpointTopic = "" }).ShouldContain("CheckpointTopic");
        RequiredError(new KafkaOptions { BootstrapServers = "kafka:9092", SecurityProtocol = "" }).ShouldContain("SecurityProtocol");
    }

    [Fact]
    public void Kafka_NonPositiveNumbers_FailRangeValidation()
    {
        RangeError(new KafkaOptions { BootstrapServers = "kafka:9092", BatchNumMessages = 0 }).ShouldContain("BatchNumMessages");
        RangeError(new KafkaOptions { BootstrapServers = "kafka:9092", LingerMs = -1 }).ShouldContain("LingerMs");
        RangeError(new KafkaOptions { BootstrapServers = "kafka:9092", DeliveryTimeoutMs = 0 }).ShouldContain("DeliveryTimeoutMs");
        RangeError(new KafkaOptions { BootstrapServers = "kafka:9092", MessageSendMaxRetries = -5 }).ShouldContain("MessageSendMaxRetries");
        RangeError(new KafkaOptions { BootstrapServers = "kafka:9092", MessageMaxBytes = 0 }).ShouldContain("MessageMaxBytes");
        RangeError(new KafkaOptions { BootstrapServers = "kafka:9092", ControlSessionTimeoutSeconds = 0 }).ShouldContain("ControlSessionTimeoutSeconds");
    }
    [Fact]
    public void Processor_NonPositiveNumbers_FailRangeValidation()
    {
        RangeError(new ProcessorOptions { QueueCapacity = 0 }).ShouldContain("QueueCapacity");
        RangeError(new ProcessorOptions { BatchSize = -1 }).ShouldContain("BatchSize");
        RangeError(new ProcessorOptions { CheckpointIntervalSeconds = 0 }).ShouldContain("CheckpointIntervalSeconds");
        RangeError(new ProcessorOptions { IdleTimeoutSeconds = 0 }).ShouldContain("IdleTimeoutSeconds");
        RangeError(new ProcessorOptions { RetryDelaySeconds = 0 }).ShouldContain("RetryDelaySeconds");
        RangeError(new ProcessorOptions { MaxRetryDelaySeconds = 0 }).ShouldContain("MaxRetryDelaySeconds");
        RangeError(new ProcessorOptions { BackoffResetSessionSeconds = -1 }).ShouldContain("BackoffResetSessionSeconds");
        RangeError(new ProcessorOptions { FinalCheckpointWaitSeconds = 0 }).ShouldContain("FinalCheckpointWaitSeconds");
        RangeError(new ProcessorOptions { ShutdownTimeoutSeconds = 0 }).ShouldContain("ShutdownTimeoutSeconds");
    }

    [Fact]
    public void FindConnectionIssue_ValidConnectionString_ReturnsNull()
    {
        PostgresOptions.FindConnectionIssue("Host=pg;Port=5433;Database=d;Username=u;Password=p;Application Name=x").ShouldBeNull();
    }

    [Fact]
    public void FindConnectionIssue_Empty_ReturnsMessage()
    {
        PostgresOptions.FindConnectionIssue("").ShouldNotBeNull();
    }

    [Fact]
    public void FindConnectionIssue_MissingParts_ReturnsMessageNamingThePart()
    {
        PostgresOptions.FindConnectionIssue("Username=u;Database=d").ShouldNotBeNull().ShouldContain("Host");
        PostgresOptions.FindConnectionIssue("Host=h;Database=d").ShouldNotBeNull().ShouldContain("Username");
        PostgresOptions.FindConnectionIssue("Host=h;Username=u").ShouldNotBeNull().ShouldContain("Database");
    }

    [Fact]
    public void FindConnectionIssue_UnknownKeyword_ReturnsMessage()
    {
        PostgresOptions.FindConnectionIssue("Host=h;Username=u;Database=d;Bogus=1").ShouldNotBeNull();
    }

    [Fact]
    public void Kafka_FindSecurityIssue_ValidCombinations_ReturnNull()
    {
        KafkaOptions.FindSecurityIssue(new KafkaOptions()).ShouldBeNull();
        KafkaOptions.FindSecurityIssue(new KafkaOptions
        {
            SecurityProtocol = "ssl",
            SslCertificateLocation = "client.pem",
            SslKeyLocation = "client.key",
        }).ShouldBeNull();
        KafkaOptions.FindSecurityIssue(new KafkaOptions
        {
            SecurityProtocol = "ssl",
            SslCaLocation = "ca.pem",
            SslCertificateLocation = "client.pem",
            SslKeyLocation = "client.key",
            SslKeyPassword = "secret",
        }).ShouldBeNull();
        KafkaOptions.FindSecurityIssue(
            new KafkaOptions { SecurityProtocol = "ssl", SslCertificateThumbprint = "ABC123" }).ShouldBeNull();
    }

    [Fact]
    public void Kafka_FindSecurityIssue_InvalidCombinations_ReturnMessage()
    {
        SecurityIssue(new KafkaOptions { SecurityProtocol = "" }).ShouldContain("SecurityProtocol");
        SecurityIssue(new KafkaOptions { SecurityProtocol = "sasl" }).ShouldContain("SecurityProtocol");
        SecurityIssue(new KafkaOptions { SecurityProtocol = "plaintext", SslCertificateThumbprint = "A" }).ShouldContain("plaintext");
        SecurityIssue(new KafkaOptions { SecurityProtocol = "ssl" }).ShouldContain("сертификат");
        SecurityIssue(new KafkaOptions
        {
            SecurityProtocol = "ssl",
            SslCertificateLocation = "client.pem",
            SslKeyLocation = "client.key",
            SslCertificateThumbprint = "A",
        }).ShouldContain("оба источника");
        SecurityIssue(new KafkaOptions { SecurityProtocol = "ssl", SslCertificateLocation = "client.pem" }).ShouldContain("вместе");
        SecurityIssue(new KafkaOptions { SecurityProtocol = "ssl", SslKeyLocation = "client.key" }).ShouldContain("сертификат");
        SecurityIssue(new KafkaOptions
        {
            SecurityProtocol = "ssl",
            SslCertificateThumbprint = "A",
            SslKeyPassword = "secret",
        }).ShouldContain("SslKeyPassword");
    }

    private static string SecurityIssue(KafkaOptions kafka) =>
        KafkaOptions.FindSecurityIssue(kafka)
        ?? throw new InvalidOperationException("Ожидалась проблема конфигурации безопасности Kafka.");


    private static IReadOnlyList<ValidationResult> Validate(object instance)
    {
        var results = new List<ValidationResult>();
        Validator.TryValidateObject(instance, new ValidationContext(instance), results, validateAllProperties: true);
        return results;
    }

    private static string RequiredError(object instance)
    {
        var error = Validate(instance).ShouldHaveSingleItem().ErrorMessage;
        return error!;
    }

    private static string RangeError(object instance)
    {
        var error = Validate(instance).ShouldHaveSingleItem().ErrorMessage;
        return error!;
    }
}
