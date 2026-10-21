using System.Runtime.InteropServices;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class SkeletonTests
{
    [Fact]
    public void Skeleton_RunsOnNet8()
    {
        Assert.StartsWith(".NET 8", RuntimeInformation.FrameworkDescription);
    }
}
