using KafkaValidator.Metrics;
using Xunit;

namespace KafkaValidator.UnitTests;

/// <summary>
/// Фиксация фактических имён метрик prometheus-net (экспозиция /metrics).
/// kafka_validator_state — обязательный liveness-gauge: счётчик без записей
/// не экспортируется вовсе.
/// Реестр prometheus-net глобален на процесс: классы, создающие KafkaValidatorMetrics,
/// идут одной коллекцией, чтобы параллельные тесты не перезаписывали gauge состояний.
/// </summary>
[Collection("prometheus-registry")]
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
