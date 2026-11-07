using CdcOutbox.Core;
using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Redundancy;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class CdcOutboxServiceCollectionExtensionsTests
{
    [Fact]
    public void AddCdcOutbox_ResolvesLeadershipArbiter_WithSlotOptions()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["CdcOutbox:Postgres:ConnectionString"] = "Host=pg;Database=outbox;Username=sys_replication",
                ["CdcOutbox:Replication:SlotName"] = "it_slot",
                ["CdcOutbox:Replication:PublicationName"] = "it_publication",
                ["CdcOutbox:Kafka:BootstrapServers"] = "kafka:9092",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddCdcOutbox(configuration);
        using var provider = services.BuildServiceProvider();

        // арбитр — hosted-сервис: резолвится из контейнера вместе с зависимостью
        // IOptions<ReplicationOptions> — имя слота, основа контрольной группы
        provider.GetServices<IHostedService>().OfType<LeadershipArbiter>().ShouldHaveSingleItem();
        provider.GetRequiredService<IOptions<ReplicationOptions>>().Value.SlotName.ShouldBe("it_slot");
    }
}
