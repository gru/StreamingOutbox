using Xunit;

namespace CdcOutbox.IntegrationTests;

[CollectionDefinition("integration")]
public sealed class IntegrationCollection : ICollectionFixture<IntegrationFixture>
{
}
