using CdcOutbox.Core.Replication;
using Shouldly;
using Xunit;

namespace CdcOutbox.UnitTests;

public sealed class LsnTextTests
{
    [Theory]
    [InlineData(0UL, "0/0")]
    [InlineData(0x12AUL, "0/12A")]
    [InlineData(0x1_0000_0002UL, "1/2")]
    [InlineData(0x1_0000_00ABUL, "1/AB")]
    [InlineData(0xFFFF_FFFF_FFFF_FFFFUL, "FFFFFFFF/FFFFFFFF")]
    public void Format_MatchesPglsnOutput(ulong value, string expected)
    {
        LsnText.Format(value).ShouldBe(expected);
    }

    [Theory]
    [InlineData("0/0", 0UL)]
    [InlineData("0/12A", 0x12AUL)]
    [InlineData("1/2", 0x1_0000_0002UL)]
    [InlineData("2/aB", 0x2_0000_00ABUL)]
    [InlineData("2/000000AB", 0x2_0000_00ABUL)]
    [InlineData("0/000000C8", 0xC8UL)]
    [InlineData("FFFFFFFF/FFFFFFFF", 0xFFFF_FFFF_FFFF_FFFFUL)]
    public void TryParse_AcceptsHexParts(string text, ulong expected)
    {
        LsnText.TryParse(text).ShouldBe(expected);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("abc")]
    [InlineData("1")]
    [InlineData("1/")]
    [InlineData("/2")]
    [InlineData("1/1234567890")]
    [InlineData("1234567890/1")]
    [InlineData("1/2/3")]
    public void TryParse_RejectsNonLsnText(string? text)
    {
        LsnText.TryParse(text).ShouldBeNull();
    }
}
