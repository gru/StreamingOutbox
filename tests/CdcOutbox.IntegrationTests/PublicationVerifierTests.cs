using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

[Collection("integration")]
public sealed class PublicationVerifierTests(IntegrationFixture fixture)
{
    private const string TargetTable = "messages";
    private const string OtherTable = "notes";

    [Fact]
    public async Task Verify_MissingPublication_Fails()
    {
        var schema = await CreateSchemaAsync();
        var publicationName = IntegrationFixture.NewName();
        var logger = new RecordingLogger<PublicationVerifier>();

        var error = await Should.ThrowAsync<InvalidOperationException>(
            () => CreateVerifier(schema, publicationName, logger).VerifyAsync(CancellationToken.None));

        error.Message.ShouldContain(publicationName);
        error.Message.ShouldContain("не найдена");
    }

    [Fact]
    public async Task Verify_PublicationWithoutInsert_Fails()
    {
        var schema = await CreateSchemaAsync();
        var publicationName = IntegrationFixture.NewName();
        await fixture.CreatePublicationAsync(publicationName, schema, new[] { TargetTable }, publish: "delete");
        var logger = new RecordingLogger<PublicationVerifier>();

        var error = await Should.ThrowAsync<InvalidOperationException>(
            () => CreateVerifier(schema, publicationName, logger).VerifyAsync(CancellationToken.None));

        error.Message.ShouldContain(publicationName);
        error.Message.ShouldContain("INSERT");
    }

    [Fact]
    public async Task Verify_PublicationWithoutOutboxTable_Fails()
    {
        var schema = await CreateSchemaAsync();
        var publicationName = IntegrationFixture.NewName();
        await fixture.CreatePublicationAsync(publicationName, schema, new[] { OtherTable });
        var logger = new RecordingLogger<PublicationVerifier>();

        var error = await Should.ThrowAsync<InvalidOperationException>(
            () => CreateVerifier(schema, publicationName, logger).VerifyAsync(CancellationToken.None));

        error.Message.ShouldContain(publicationName);
        error.Message.ShouldContain("не входит");
    }

    [Fact]
    public async Task Verify_ExtraOperations_Warns()
    {
        var schema = await CreateSchemaAsync();
        var publicationName = IntegrationFixture.NewName();
        await fixture.CreatePublicationAsync(publicationName, schema, new[] { TargetTable }, publish: "insert, update");
        var logger = new RecordingLogger<PublicationVerifier>();

        await CreateVerifier(schema, publicationName, logger).VerifyAsync(CancellationToken.None);

        logger.Entries.ShouldContain(entry =>
            entry.Level == LogLevel.Warning && entry.Message.Contains(publicationName) && entry.Message.Contains("не только INSERT"));
    }

    [Fact]
    public async Task Verify_InsertOnlyPublication_PassesWithoutWarning()
    {
        var schema = await CreateSchemaAsync();
        var publicationName = IntegrationFixture.NewName();
        await fixture.CreatePublicationAsync(publicationName, schema, new[] { TargetTable });
        var logger = new RecordingLogger<PublicationVerifier>();

        await CreateVerifier(schema, publicationName, logger).VerifyAsync(CancellationToken.None);

        logger.Entries.ShouldNotContain(entry => entry.Level == LogLevel.Warning);
        logger.Entries.ShouldContain(entry =>
            entry.Level == LogLevel.Information && entry.Message.Contains(publicationName) && entry.Message.Contains("INSERT"));
    }

    private async Task<string> CreateSchemaAsync()
    {
        var schema = IntegrationFixture.NewSchemaName();
        await fixture.CreateSchemaAsync(schema);
        await fixture.CreateOutboxMessagesTableAsync(schema);
        await fixture.CreateExtraTableAsync(schema, OtherTable);
        return schema;
    }

    private PublicationVerifier CreateVerifier(string schema, string publicationName, RecordingLogger<PublicationVerifier> logger) =>
        new(
            fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection),
            fixture.BuildOptions<ReplicationOptions>(
                ReplicationOptions.ConfigurationSection,
                (ReplicationOptions.ConfigurationSection + ":SlotName", "it_slot"),
                (ReplicationOptions.ConfigurationSection + ":PublicationName", publicationName)),
            fixture.BuildOptions<OutboxStorageOptions>(
                OutboxStorageOptions.ConfigurationSection,
                (OutboxStorageOptions.ConfigurationSection + ":Schema", schema),
                (OutboxStorageOptions.ConfigurationSection + ":Table", TargetTable)),
            logger);
}
