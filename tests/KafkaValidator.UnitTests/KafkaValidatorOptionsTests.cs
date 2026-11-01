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
        o.Writer.BaseUrl.ShouldBe("http://writer:8080");
    }
}
