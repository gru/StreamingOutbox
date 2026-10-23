using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace CdcOutbox.IntegrationTests;

[Collection("integration")]
public sealed class OutboxSlotProvisionerTests(IntegrationFixture fixture)
{
    [Fact]
    public async Task Ensure_MissingSlot_CreatesPgoutputFailover()
    {
        var slotName = IntegrationFixture.NewName();
        var logger = new RecordingLogger<OutboxSlotProvisioner>();

        await CreateProvisioner(slotName, logger).EnsureAsync(CancellationToken.None);

        var slot = (await fixture.GetSlotsAsync(slotName)).Single();
        slot.SlotName.ShouldBe(slotName);
        slot.Plugin.ShouldBe("pgoutput");
        slot.Failover.ShouldNotBeNull();
        slot.Failover.Value.ShouldBeTrue();
        logger.Entries.ShouldContain(entry =>
            entry.Level == LogLevel.Information && entry.Message.Contains(slotName) && entry.Message.Contains("создан"));
    }

    [Fact]
    public async Task Ensure_SecondRun_AcceptsExistingSlotAndKeepsSingle()
    {
        var slotName = IntegrationFixture.NewName();
        var logger = new RecordingLogger<OutboxSlotProvisioner>();
        var provisioner = CreateProvisioner(slotName, logger);

        await provisioner.EnsureAsync(CancellationToken.None);
        await provisioner.EnsureAsync(CancellationToken.None);

        (await fixture.GetSlotsAsync(slotName)).Count.ShouldBe(1);
        logger.Entries.ShouldContain(entry =>
            entry.Level == LogLevel.Information && entry.Message.Contains(slotName) && entry.Message.Contains("уже существует"));
    }

    [Fact]
    public async Task Ensure_SlotWithOtherPlugin_Fails()
    {
        var slotName = IntegrationFixture.NewName();
        await fixture.CreateLogicalSlotAsync(slotName, "test_decoding");
        var logger = new RecordingLogger<OutboxSlotProvisioner>();

        var error = await Should.ThrowAsync<InvalidOperationException>(
            () => CreateProvisioner(slotName, logger).EnsureAsync(CancellationToken.None));

        error.Message.ShouldContain(slotName);
        error.Message.ShouldContain("pgoutput");
        error.Message.ShouldContain("test_decoding");
    }

    [Fact]
    public async Task Ensure_SlotWithoutFailover_Warns()
    {
        var slotName = IntegrationFixture.NewName();
        await fixture.CreateLogicalSlotAsync(slotName, failover: false);
        var logger = new RecordingLogger<OutboxSlotProvisioner>();

        await CreateProvisioner(slotName, logger).EnsureAsync(CancellationToken.None);

        logger.Entries.ShouldContain(entry =>
            entry.Level == LogLevel.Warning && entry.Message.Contains(slotName) && entry.Message.Contains("promotion"));
    }

    private OutboxSlotProvisioner CreateProvisioner(string slotName, RecordingLogger<OutboxSlotProvisioner> logger) =>
        new(
            fixture.BuildOptions<PostgresOptions>(PostgresOptions.ConfigurationSection),
            fixture.BuildOptions<ReplicationOptions>(
                ReplicationOptions.ConfigurationSection,
                (ReplicationOptions.ConfigurationSection + ":SlotName", slotName),
                (ReplicationOptions.ConfigurationSection + ":PublicationName", "it_publication")),
            logger);
}
