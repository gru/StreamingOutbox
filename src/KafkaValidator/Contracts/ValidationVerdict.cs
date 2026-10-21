namespace KafkaValidator.Contracts;

/// <summary>
/// Вердикт валидации прогона — фактическое содержимое Kafka против ожидаемого
/// набора id. Минимум: общее количество записей, уникальные id, дубликаты,
/// отсутствующие id, записанные Writer.
///
/// Семантика потерь («отделяй неопределённые результаты записи от доказанных
/// потерь доставки»):
///   MissingWrittenIds      — Writer ДОКАЗАЛ commit (written), записи в Kafka
///                            нет → доказанная потеря доставки;
///   Ambiguous*             — исход commit у Writer не определён: и доставленные
///                            (commit всё-таки случился), и недоставленные
///                            (скорее всего commit не случился) НЕ являются
///                            доказанной потерей;
///   UnexpectedExtra        — id в Kafka вне всех списков Writer — аномалия
///                            (доказанная лишняя доставка);
///   DuplicateRecords       — записи сверх уникальных id: допустимо (at-least-once),
///                            на вердикт не влияет.
/// Вердикт pass ⇔ нет доказанных потерь и нет неожидаемых лишних id.
/// </summary>
public sealed class ValidationVerdict
{
    /// <summary>
    /// Идентификатор прогона.
    /// </summary>
    public Guid RunId { get; set; }

    /// <summary>
    /// pass | fail.
    /// </summary>
    public string Verdict { get; set; } = "";

    /// <summary>
    /// Момент вычисления вердикта (UTC).
    /// </summary>
    public DateTimeOffset ComputedAtUtc { get; set; }

    /// <summary>
    /// Фактические записи прогона в Kafka.
    /// </summary>
    public KafkaCounts Kafka { get; set; } = new();

    /// <summary>
    /// Ожидаемый набор, против которого сверяли.
    /// </summary>
    public ExpectedCounts Expected { get; set; } = new();

    /// <summary>
    /// Доказанные потери: written в Writer, отсутствуют в Kafka.
    /// </summary>
    public int MissingWrittenIdsCount { get; set; }

    /// <summary>
    /// Первые 20 отсутствующих id (полный список не влезает в ответ при массовой потере).
    /// </summary>
    public List<long> MissingWrittenIdsSample { get; set; } = new();

    /// <summary>
    /// Неопределённые коммиты Writer, которые ДОСТАВЛЕНЫ (commit всё-таки был).
    /// </summary>
    public int AmbiguousDeliveredCount { get; set; }

    /// <summary>
    /// Неопределённые коммиты Writer, которых в Kafka нет (не потеря: вероятно, commit не был зафиксирован).
    /// </summary>
    public int AmbiguousNotDeliveredCount { get; set; }

    /// <summary>
    /// Записи в Kafka с id вне written/ambiguous — аномалия: Writer не отправлял
    /// такого сообщения. В частности, доставленный failed-id невозможен при
    /// корректной доставке (commit не зафиксирован → записи в WAL нет).
    /// </summary>
    public int UnexpectedExtraIdsCount { get; set; }

    /// <summary>
    /// Первые 20 неожиданных id (полный список не влезает в ответ).
    /// </summary>
    public List<long> UnexpectedExtraIdsSample { get; set; } = new();

    /// <summary>
    /// Снапшот outbox-сервиса на момент валидации (подтверждено доставкой).
    /// Пока outbox-сервис не реализован — всегда null; поле сохранено под будущее
    /// обогащение (аналог /api/cdc/state из исследовательского стенда).
    /// </summary>
    public CdcSnapshot? Cdc { get; set; }

    /// <summary>
    /// Фактические счётчики прогона в Kafka.
    /// </summary>
    public sealed class KafkaCounts
    {
        /// <summary>
        /// Всего записей прогона (дубликаты включены).
        /// </summary>
        public long Records { get; set; }

        /// <summary>
        /// Уникальных id.
        /// </summary>
        public int UniqueIds { get; set; }

        /// <summary>
        /// Записей сверх уникальных id (= Records − UniqueIds).
        /// </summary>
        public long DuplicateRecords { get; set; }
    }

    /// <summary>
    /// Счётчики ожидаемого набора.
    /// </summary>
    public sealed class ExpectedCounts
    {
        /// <summary>
        /// writer | explicit.
        /// </summary>
        public string Source { get; set; } = "";

        /// <summary>
        /// Число written id.
        /// </summary>
        public int WrittenCount { get; set; }

        /// <summary>
        /// Число ambiguous id.
        /// </summary>
        public int AmbiguousCount { get; set; }

        /// <summary>
        /// Число failed id.
        /// </summary>
        public int FailedCount { get; set; }
    }

    /// <summary>
    /// Снапшот outbox-сервиса на момент валидации.
    /// </summary>
    public sealed class CdcSnapshot
    {
        /// <summary>
        /// Идентификатор сервиса-доставщика.
        /// </summary>
        public string Arm { get; set; } = "";

        /// <summary>
        /// Состояние сервиса-доставщика.
        /// </summary>
        public string State { get; set; } = "";

        /// <summary>
        /// Доставлено сообщений (по подтверждениям сервиса).
        /// </summary>
        public long MessagesDelivered { get; set; }

        /// <summary>
        /// Подтвержденный LSN слота.
        /// </summary>
        public string? ConfirmedLsn { get; set; }

        /// <summary>
        /// Ошибок публикации в Kafka.
        /// </summary>
        public long KafkaErrors { get; set; }

        /// <summary>
        /// Ошибок чтения WAL.
        /// </summary>
        public long ReadErrors { get; set; }
    }
}
