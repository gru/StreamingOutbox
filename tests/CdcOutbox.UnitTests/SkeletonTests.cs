using System.Runtime.InteropServices;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class SkeletonTests
{
    [Fact]
    public void Skeleton_RunsOnNet8()
    {
        RuntimeInformation.FrameworkDescription.ShouldStartWith(".NET 8");
    }
}
