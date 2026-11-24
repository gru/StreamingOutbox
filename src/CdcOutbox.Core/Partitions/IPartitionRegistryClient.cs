namespace CdcOutbox.Core.Partitions;

/// <summary>
/// Операции обслуживания часовых партиций и защитного курсора доставки в PostgreSQL.
/// </summary>
public interface IPartitionRegistryClient
{
    /// <summary>
    /// Получить уникальный идентификатор кластера PostgreSQL (pg_control_system.system_identifier).
    /// </summary>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Паспорт кластера PostgreSQL.</returns>
    Task<long> GetSystemIdentifierAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Проверить горизонт будущих партиций и идемпотентно досоздать недостающие.
    /// </summary>
    /// <param name="nowUtc">Текущее время UTC.</param>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Количество созданных партиций и текущий запас будущих часов.</returns>
    Task<PartitionEnsureResult> EnsurePartitionsAsync(DateTimeOffset nowUtc, CancellationToken cancellationToken);

    /// <summary>
    /// Запечатать закрывшиеся часовые партиции текущим LSN журнала (после окна grace).
    /// </summary>
    /// <param name="nowUtc">Текущее время UTC.</param>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Количество запечатанных партиций.</returns>
    Task<int> SealClosedPartitionsAsync(DateTimeOffset nowUtc, CancellationToken cancellationToken);

    /// <summary>
    /// Прочитать состояние защитного курсора слота.
    /// </summary>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Состояние защитного курсора; null, если запись ещё не инициализирована.</returns>
    Task<PartitionGuardState?> ReadGuardStateAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Идемпотентно создать запись защитного курсора для слота, если она ещё отсутствует.
    /// </summary>
    /// <param name="systemIdentifier">Паспорт кластера PostgreSQL.</param>
    /// <param name="initialLsn">Начальный подтверждённый LSN (если известен).</param>
    /// <param name="initialCreatedAtUtc">Начальная метка created_at доставленных сообщений (если известна).</param>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Задача инициализации.</returns>
    Task EnsureGuardInitializedAsync(
        long systemIdentifier,
        ulong? initialLsn,
        DateTime? initialCreatedAtUtc,
        CancellationToken cancellationToken);

    /// <summary>
    /// Монотонно обновить непрерывный доставленный прогресс (только если recovery_in_progress=false и совпадает system_identifier).
    /// </summary>
    /// <param name="contiguousDeliveredLsn">Подтверждённо доставленный непрерывный LSN.</param>
    /// <param name="lastDeliveredCreatedAtUtc">Максимальный created_at доставленных сообщений.</param>
    /// <param name="systemIdentifier">Текущий паспорт кластера PostgreSQL.</param>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Задача обновления.</returns>
    Task UpdateDeliveryProgressAsync(
        ulong contiguousDeliveredLsn,
        DateTime? lastDeliveredCreatedAtUtc,
        long systemIdentifier,
        CancellationToken cancellationToken);

    /// <summary>
    /// Установить или снять блокировку удаления партиций на время авто-восстановления дыры.
    /// </summary>
    /// <param name="inProgress">true — заблокировать удаление; false — разблокировать после полной вычитки дыры.</param>
    /// <param name="systemIdentifier">Текущий паспорт кластера PostgreSQL.</param>
    /// <param name="resetContiguousLsn">Новый непрерывный LSN при снятии блокировки.</param>
    /// <param name="resetLastCreatedAtUtc">Новая метка created_at при снятии блокировки.</param>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Задача изменения блокировки.</returns>
    Task SetRecoveryLockAsync(
        bool inProgress,
        long systemIdentifier,
        ulong? resetContiguousLsn,
        DateTime? resetLastCreatedAtUtc,
        CancellationToken cancellationToken);

    /// <summary>
    /// Безопасно удалить запечатанные и полностью доставленные партиции, прошедшие окно хранения.
    /// Не удаляет ничего при recovery_in_progress=true, смене system_identifier или отсутствии продвижения LSN.
    /// </summary>
    /// <param name="safeContiguousLsn">Подтверждённый непрерывный LSN процесса.</param>
    /// <param name="systemIdentifier">Текущий паспорт кластера PostgreSQL.</param>
    /// <param name="nowUtc">Текущее время UTC.</param>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Имена удалённых партиций.</returns>
    Task<IReadOnlyList<string>> DropDeliveredPartitionsAsync(
        ulong safeContiguousLsn,
        long systemIdentifier,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken);
}
