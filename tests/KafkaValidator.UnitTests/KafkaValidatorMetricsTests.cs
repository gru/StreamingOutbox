using KafkaValidator.Metrics;
using Shouldly;
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

        metrics.RecordsConsumed.Name.ShouldBe("kafka_validator_records_consumed_total");
        metrics.ParseErrors.Name.ShouldBe("kafka_validator_parse_errors_total");
        metrics.SkippedRecords.Name.ShouldBe("kafka_validator_skipped_records_total");
        metrics.Validations.Name.ShouldBe("kafka_validator_validations_total");
        metrics.State.Name.ShouldBe("kafka_validator_state");
        metrics.RunsSeen.Name.ShouldBe("kafka_validator_runs_seen");

        metrics.ObserveState(KafkaValidatorMetrics.StateRunning);
        ((int)metrics.State.Value).ShouldBe(KafkaValidatorMetrics.StateRunning);
        metrics.ObserveRunsSeen(3);
        metrics.RunsSeen.Value.ShouldBe(3);
    }
}
