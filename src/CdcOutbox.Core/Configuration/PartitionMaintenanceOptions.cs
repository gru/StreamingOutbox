using System.ComponentModel.DataAnnotations;

namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Настройки фонового управления почасовыми партициями и 24/7 авто-восстановления из них.
/// </summary>
public sealed class PartitionMaintenanceOptions
{
    /// <summary>
    /// Путь секции конфигурации.
    /// </summary>
    public const string ConfigurationSection = "CdcOutbox:Partitions";

    /// <summary>
    /// Включить менеджер почасовых партиций (автоматически активен при <see cref="OutboxStorageMode.PartitionedTable"/>).
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Горизонт предварительного создания часовых партиций вперёд от текущего часа, ч (по умолчанию 48 ч = 2 дня).
    /// </summary>
    [Range(1, 720, ErrorMessage = "CdcOutbox:Partitions:PrecreateHours должен быть в диапазоне [1..720].")]
    public int PrecreateHours { get; set; } = 48;

    /// <summary>
    /// Срок хранения закрытой и полностью доставленной партиции перед удалением, ч (0 — удалять сразу в ближайший запуск после подтверждения доставки).
    /// </summary>
    [Range(0, 720, ErrorMessage = "CdcOutbox:Partitions:RetentionHours должен быть в диапазоне [0..720].")]
    public int RetentionHours { get; set; }

    /// <summary>
    /// Минута каждого часа UTC, в которую запускается плановый цикл обслуживания (по умолчанию 5 → HH:05:00 UTC).
    /// </summary>
    [Range(0, 59, ErrorMessage = "CdcOutbox:Partitions:RunAtMinuteOfHour должен быть в диапазоне [0..59].")]
    public int RunAtMinuteOfHour { get; set; } = 5;

    /// <summary>
    /// Пауза после окончания часа перед запечатыванием верхней границы LSN партиции, мин.
    /// </summary>
    [Range(0, 59, ErrorMessage = "CdcOutbox:Partitions:SealGraceMinutes должен быть в диапазоне [0..59].")]
    public int SealGraceMinutes { get; set; } = 5;

    /// <summary>
    /// Автоматически восстанавливать пропущенные сообщения из сохранённых партиций при потере или инвалидации слота (24/7 без ручного вмешательства).
    /// </summary>
    public bool AutoRecoverFromPartitions { get; set; } = true;

    /// <summary>
    /// Размер пачки чтения и публикации при авто-восстановлении из партиций.
    /// </summary>
    [Range(1, 50000, ErrorMessage = "CdcOutbox:Partitions:RecoveryBatchSize должен быть в диапазоне [1..50000].")]
    public int RecoveryBatchSize { get; set; } = 1000;

    /// <summary>
    /// Запас перекрытия назад по created_at при авто-восстановлении дыры из партиций, мин.
    /// </summary>
    [Range(0, 1440, ErrorMessage = "CdcOutbox:Partitions:RecoveryOverlapMinutes должен быть в диапазоне [0..1440].")]
    public int RecoveryOverlapMinutes { get; set; } = 5;

    /// <summary>
    /// Выполнять DDL создания/удаления партиций через SECURITY DEFINER-функции БД (true — минимум прав роли сервиса)
    /// либо напрямую от имени владельца таблицы (false — использует DETACH PARTITION CONCURRENTLY).
    /// </summary>
    public bool UseSecurityDefinerFunctions { get; set; } = true;

    /// <summary>
    /// Имя таблицы реестра партиций в схеме outbox.
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Partitions:RegistryTable не задана.")]
    public string RegistryTable { get; set; } = "partition_registry";

    /// <summary>
    /// Имя служебной таблицы защиты доставки (checkpoint/system_identifier/lock) в схеме outbox.
    /// </summary>
    [Required(ErrorMessage = "CdcOutbox:Partitions:GuardTable не задана.")]
    public string GuardTable { get; set; } = "partition_guard";
}
