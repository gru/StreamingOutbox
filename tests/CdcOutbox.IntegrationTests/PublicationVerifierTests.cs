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
        var publicationName = NewName();
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
        var publicationName = NewName();
        await fixture.ExecuteAsync(
            $"CREATE PUBLICATION {publicationName} FOR TABLE {schema}.{TargetTable} WITH (publish = 'delete')");
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
        var publicationName = NewName();
        await fixture.ExecuteAsync(
            $"CREATE PUBLICATION {publicationName} FOR TABLE {schema}.{OtherTable} WITH (publish = 'insert')");
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
        var publicationName = NewName();
        await fixture.ExecuteAsync(
            $"CREATE PUBLICATION {publicationName} FOR TABLE {schema}.{TargetTable} WITH (publish = 'insert, update')");
        var logger = new RecordingLogger<PublicationVerifier>();

        await CreateVerifier(schema, publicationName, logger).VerifyAsync(CancellationToken.None);

        logger.Entries.ShouldContain(entry =>
            entry.Level == LogLevel.Warning && entry.Message.Contains(publicationName) && entry.Message.Contains("не только INSERT"));
    }

    [Fact]
    public async Task Verify_InsertOnlyPublication_PassesWithoutWarning()
    {
        var schema = await CreateSchemaAsync();
        var publicationName = NewName();
        await fixture.ExecuteAsync(
            $"CREATE PUBLICATION {publicationName} FOR TABLE {schema}.{TargetTable} WITH (publish = 'insert')");
        var logger = new RecordingLogger<PublicationVerifier>();

        await CreateVerifier(schema, publicationName, logger).VerifyAsync(CancellationToken.None);

        logger.Entries.ShouldNotContain(entry => entry.Level == LogLevel.Warning);
        logger.Entries.ShouldContain(entry =>
            entry.Level == LogLevel.Information && entry.Message.Contains(publicationName) && entry.Message.Contains("INSERT"));
    }

    private static string NewName() => $"it_{Guid.NewGuid():N}";

    private async Task<string> CreateSchemaAsync()
    {
        var schema = $"it_s_{Guid.NewGuid():N}";
        await fixture.ExecuteAsync($"CREATE SCHEMA {schema}");
        await fixture.ExecuteAsync($"CREATE TABLE {schema}.{TargetTable} (id uuid PRIMARY KEY, body text, created_at timestamptz)");
        await fixture.ExecuteAsync($"CREATE TABLE {schema}.{OtherTable} (id uuid PRIMARY KEY, body text)");
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
