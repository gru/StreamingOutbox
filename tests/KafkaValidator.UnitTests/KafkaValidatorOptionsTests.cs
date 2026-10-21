using Xunit;
using KafkaValidator.Configuration;
using KafkaValidator.Metrics;

namespace KafkaValidator.UnitTests;

public class KafkaValidatorOptionsTests
{
    [Fact]
    public void Defaults_MatchStand()
    {
        var o = new KafkaValidatorOptions();

        Assert.Equal("kafka1:9092,kafka2:9092,kafka3:9092", o.Kafka.Brokers);
        Assert.Equal(["kafka1:9092", "kafka2:9092", "kafka3:9092"], o.Kafka.BrokerList());
        Assert.Equal("outbox.events", o.Kafka.Topic);
        Assert.Equal("kafka-validator", o.Kafka.GroupId);
        Assert.Equal(1000, o.Kafka.BufferSize);
        Assert.Equal(1, o.Kafka.WorkersCount);
        Assert.Equal("http://writer:8080", o.Writer.BaseUrl);
    }

    [Theory]
    [InlineData(0, 100)]
    [InlineData(99, 100)]
    [InlineData(100, 100)]
    [InlineData(5000, 5000)]
    [InlineData(1_000_000, 100_000)]
    public void ClampBufferSize_Bounds(int value, int expected)
    {
        Assert.Equal(expected, new KafkaOptions { BufferSize = value }.ClampBufferSize());
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(4, 4)]
    [InlineData(99, 16)]
    public void ClampWorkersCount_Bounds(int value, int expected)
    {
        Assert.Equal(expected, new KafkaOptions { WorkersCount = value }.ClampWorkersCount());
    }
}

/// <summary>
/// Фиксация фактических имён метрик prometheus-net (экспозиция /metrics).
/// kafka_validator_state — обязательный liveness-gauge: счётчик без записей
/// не экспортируется вовсе.
/// </summary>
public class KafkaValidatorMetricsTests
{
    [Fact]
    public void MetricNames_AndStateGauge()
    {
        var metrics = new KafkaValidatorMetrics(); // единственная регистрация в default-registry процесса

        Assert.Equal("kafka_validator_records_consumed_total", metrics.RecordsConsumed.Name);
        Assert.Equal("kafka_validator_parse_errors_total", metrics.ParseErrors.Name);
        Assert.Equal("kafka_validator_skipped_records_total", metrics.SkippedRecords.Name);
        Assert.Equal("kafka_validator_validations_total", metrics.Validations.Name);
        Assert.Equal("kafka_validator_state", metrics.State.Name);
        Assert.Equal("kafka_validator_runs_seen", metrics.RunsSeen.Name);

        metrics.ObserveState(KafkaValidatorMetrics.StateRunning);
        Assert.Equal(KafkaValidatorMetrics.StateRunning, (int)metrics.State.Value);
        metrics.ObserveRunsSeen(3);
        Assert.Equal(3, metrics.RunsSeen.Value);
    }
}
