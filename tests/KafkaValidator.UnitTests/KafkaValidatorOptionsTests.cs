using KafkaValidator.Configuration;
using Shouldly;
using Xunit;

namespace KafkaValidator.UnitTests;

public class KafkaValidatorOptionsTests
{
    [Fact]
    public void Defaults_MatchStand()
    {
        var o = new KafkaValidatorOptions();

        o.Kafka.Brokers.ShouldBe("kafka1:9092,kafka2:9092,kafka3:9092");
        o.Kafka.BrokerList().ShouldBe(["kafka1:9092", "kafka2:9092", "kafka3:9092"]);
        o.Kafka.Topic.ShouldBe("outbox.events");
        o.Kafka.GroupId.ShouldBe("kafka-validator");
        o.Kafka.BufferSize.ShouldBe(1000);
        o.Kafka.WorkersCount.ShouldBe(1);
        o.Writer.BaseUrl.ShouldBe("http://writer:8080");
    }

    [Theory]
    [InlineData(0, 100)]
    [InlineData(99, 100)]
    [InlineData(100, 100)]
    [InlineData(5000, 5000)]
    [InlineData(1_000_000, 100_000)]
    public void ClampBufferSize_Bounds(int value, int expected)
    {
        new KafkaOptions { BufferSize = value }.ClampBufferSize().ShouldBe(expected);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(4, 4)]
    [InlineData(99, 16)]
    public void ClampWorkersCount_Bounds(int value, int expected)
    {
        new KafkaOptions { WorkersCount = value }.ClampWorkersCount().ShouldBe(expected);
    }
}
