using KafkaValidator.Contracts;

namespace KafkaValidator.Validation;

/// <summary>
/// Накопленные счётчики одного прогона (runId) по фактическим записям Kafka.
/// Потоковая безопасность обеспечивается внешним lock RunRegistry
/// (воркер консюмера один, но реестр читают и HTTP-эндпоинты).
/// </summary>
internal sealed class RunTallies(Guid runId)
{
    private readonly HashSet<long> _ids = new();

    public Guid RunId { get; } = runId;

    public long Records { get; private set; }

    public DateTimeOffset FirstSeenUtc { get; private set; }

    public DateTimeOffset LastSeenUtc { get; private set; }

    public void Record(long messageId, DateTimeOffset nowUtc)
    {
        Records++;
        _ids.Add(messageId);
        if (Records == 1)
            FirstSeenUtc = nowUtc;
        LastSeenUtc = nowUtc;
    }

    public RunSummary ToSummary() => new(RunId, Records, _ids.Count, Records - _ids.Count, FirstSeenUtc, LastSeenUtc);

    public RunDetail ToDetail() => new(RunId, Records, _ids, FirstSeenUtc, LastSeenUtc);
}
