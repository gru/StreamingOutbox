using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Partitions;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Processing;

/// <summary>
/// Стартовые проверки объектов доставки; выполняет только лидер после
/// инициализации продюсера, любая ошибка останавливает хост.
/// В режиме <see cref="OutboxStorageMode.PartitionedTable"/> с включённым
/// авто-восстановлением автоматически пересоздаёт потерянный/инвалидированный слот
/// и дочитывает пропущенное окно из сохранённых часовых партиций.
/// </summary>
public sealed class OutboxStartupChecks(
    OutboxSlotProvisioner slotProvisioner,
    PublicationVerifier publicationVerifier,
    CheckpointTopicVerifier checkpointTopicVerifier,
    CheckpointStore checkpointStore,
    OutboxMetrics metrics,
    IOptions<CdcOutboxOptions> options,
    ILogger<OutboxStartupChecks> logger,
    IOptions<OutboxStorageOptions>? storageOptions = null,
    IOptions<PartitionMaintenanceOptions>? partitionOptions = null,
    PartitionRecoveryService? partitionRecovery = null,
    IPartitionRegistryClient? partitionRegistry = null,
    ReplicationProgress? progress = null,
    TimeProvider? timeProvider = null) : IOutboxStartupChecks
{
    private readonly CdcOutboxOptions _options = options.Value;
    private readonly OutboxStorageMode _storageMode = storageOptions?.Value.Mode ?? OutboxStorageMode.Table;
    private readonly bool _autoRecoverFromPartitions =
        (storageOptions?.Value.Mode == OutboxStorageMode.PartitionedTable)
        && (partitionOptions?.Value.AutoRecoverFromPartitions ?? true)
        && partitionRecovery is not null
        && partitionRegistry is not null;

    /// <summary>
    /// Прогнать стартовые проверки.
    /// </summary>
    /// <param name="cancellationToken">Токен остановки.</param>
    /// <returns>Задача проверок.</returns>
    public async Task RunAllAsync(CancellationToken cancellationToken)
    {
        checkpointTopicVerifier.Verify();

        if (_autoRecoverFromPartitions)
        {
            await RunPartitionedStartupAsync(cancellationToken);
            await publicationVerifier.VerifyAsync(cancellationToken);
            return;
        }

        if (_options.RecoveryMode)
        {
            await RunRecoveryAsync(cancellationToken);
        }
        else
        {
            await ReconcileCheckpointWithSlotAsync(cancellationToken);
            await slotProvisioner.EnsureAsync(cancellationToken);
        }

        await publicationVerifier.VerifyAsync(cancellationToken);
    }

    private async Task RunPartitionedStartupAsync(CancellationToken cancellationToken)
    {
        var now = (timeProvider ?? TimeProvider.System).GetUtcNow();
        var ensured = await partitionRegistry!.EnsurePartitionsAsync(now, cancellationToken);
        metrics.SetFuturePartitionsCount(ensured.FuturePartitionsCount);
        metrics.RegisterPartitionsCreated(ensured.CreatedCount);

        var checkpoint = checkpointStore.ReadLatest(cancellationToken);
        var slot = await slotProvisioner.FindSlotAsync(cancellationToken);
        var guard = await partitionRegistry.ReadGuardStateAsync(cancellationToken);
        var systemIdentifier = await partitionRegistry.GetSystemIdentifierAsync(cancellationToken);

        var recoveryReason = await DetectPartitionRecoveryReasonAsync(
            checkpoint,
            slot,
            guard,
            systemIdentifier,
            cancellationToken);

        if (recoveryReason is not null)
        {
            await partitionRecovery!.RecoverAsync(checkpoint, recoveryReason, cancellationToken);
            return;
        }

        await slotProvisioner.EnsureAsync(cancellationToken);

        var effectiveLsn = checkpoint ?? await slotProvisioner.GetConfirmedFlushAsync(cancellationToken);
        await partitionRegistry.EnsureGuardInitializedAsync(systemIdentifier, effectiveLsn, null, cancellationToken);

        if (checkpoint is { } verified && progress is not null)
        {
            progress.MarkRead(verified);
            progress.MarkDelivered(verified);
            progress.MarkCheckpointed(verified, now.UtcDateTime);
            await partitionRegistry.UpdateDeliveryProgressAsync(verified, null, systemIdentifier, cancellationToken);
        }

        if (checkpoint is { } saved && slot is not null)
        {
            var diff = await slotProvisioner.GetConfirmedFlushDiffAsync(saved, cancellationToken);
            if (diff < 0)
            {
                logger.LogWarning(
                    "Checkpoint {CheckpointLsn} опережает confirmed_flush слота на {DiffBytes} байт — продолжение с позиции слота.",
                    LsnText.Format(saved),
                    -diff.Value);
            }
            else if (diff == 0)
            {
                logger.LogInformation(
                    "Checkpoint {CheckpointLsn} согласован с confirmed_flush слота — продолжение с позиции слота.",
                    LsnText.Format(saved));
            }
        }
    }

    private async Task<string?> DetectPartitionRecoveryReasonAsync(
        ulong? checkpoint,
        SlotCatalogEntry? slot,
        PartitionGuardState? guard,
        long currentSystemIdentifier,
        CancellationToken cancellationToken)
    {
        if (guard is { RecoveryInProgress: true })
        {
            return "предыдущее авто-восстановление из партиций не завершилось (recovery_in_progress=true)";
        }

        if (slot is { IsInvalidated: true })
        {
            return $"слот инвалидирован PostgreSQL (wal_status='{slot.WalStatus}', reason='{slot.InvalidationReason}')";
        }

        if (guard is not null && guard.SystemIdentifier != currentSystemIdentifier)
        {
            return $"изменился system_identifier кластера PostgreSQL ({guard.SystemIdentifier} → {currentSystemIdentifier})";
        }

        if (checkpoint is not { } saved)
        {
            return null;
        }

        if (slot is null)
        {
            return $"слот отсутствует при сохранённом checkpoint {LsnText.Format(saved)}";
        }

        var diff = await slotProvisioner.GetConfirmedFlushDiffAsync(saved, cancellationToken);
        if (diff is null)
        {
            return $"confirmed_flush существующего слота пуст при сохранённом checkpoint {LsnText.Format(saved)}";
        }

        if (diff > 0)
        {
            return $"confirmed_flush слота опережает checkpoint {LsnText.Format(saved)} на {diff.Value} байт (обнаружен разрыв)";
        }

        return null;
    }

    private async Task RunRecoveryAsync(CancellationToken cancellationToken)
    {
        var checkpoint = checkpointStore.ReadLatest(cancellationToken);
        var slot = await slotProvisioner.FindSlotAsync(cancellationToken);
        var confirmedFlush = slot is null ? null : await slotProvisioner.GetConfirmedFlushAsync(cancellationToken);
        var decision = RecoveryPlanner.Plan(checkpoint, slot is not null, confirmedFlush);
        if (decision.Action == RecoveryAction.Reject)
        {
            throw new StartupCheckException(decision.Description);
        }

        // слот обеспечивается до сохранения checkpoint: отсутствующий создаётся
        // с текущей позиции WAL — его позиция и станет новым checkpoint
        await slotProvisioner.EnsureAsync(cancellationToken);

        if (decision.Action == RecoveryAction.NotNeeded)
        {
            logger.LogWarning(
                "Recovery Mode включён, но {Description}; checkpoint не переписывается.",
                decision.Description);
            return;
        }

        var newPosition = decision.Action == RecoveryAction.AdoptSlotPosition
            ? confirmedFlush
            : await slotProvisioner.GetConfirmedFlushAsync(cancellationToken);
        if (newPosition is not { } target)
        {
            throw new StartupCheckException(
                "После обеспечения слота его confirmed_flush пуст — позиция для recovery-checkpoint недоступна.");
        }

        // новый checkpoint сохраняется до начала чтения: последующие перезапуски
        // проходят штатное согласование уже без recovery
        await checkpointStore.SaveTransactionAsync(target, cancellationToken);
        logger.LogWarning(
            "Recovery Mode ({Mode}): checkpoint {OldLsn} → {NewLsn}; события в окне между позициями пропускаются — " +
            "осознанный отказ от их доставки по решению оператора. После восстановления перезапусти сервис без флага.",
            _storageMode,
            checkpoint is { } saved ? LsnText.Format(saved) : "<не было>",
            LsnText.Format(target));
    }

    private async Task ReconcileCheckpointWithSlotAsync(CancellationToken cancellationToken)
    {
        var checkpoint = checkpointStore.ReadLatest(cancellationToken);
        var slot = await slotProvisioner.FindSlotAsync(cancellationToken);
        if (checkpoint is not { } saved)
        {
            return;
        }

        var diff = await slotProvisioner.GetConfirmedFlushDiffAsync(saved, cancellationToken);
        var issue = CheckpointReconciler.FindIssue(slot is not null, diff, saved);
        if (issue is not null)
        {
            throw new StartupCheckException(issue);
        }

        if (diff < 0)
        {
            logger.LogWarning(
                "Checkpoint {CheckpointLsn} опережает confirmed_flush слота на {DiffBytes} байт — продолжение с позиции слота.",
                LsnText.Format(saved),
                -diff.Value);
        }
        else if (diff == 0)
        {
            logger.LogInformation(
                "Checkpoint {CheckpointLsn} согласован с confirmed_flush слота — продолжение с позиции слота.",
                LsnText.Format(saved));
        }
        else
        {
            // позиция слота принимается как checkpoint до начала чтения: окно между
            // позициями нечитаемо, пропуск фиксируется durably
            var flush = await slotProvisioner.GetConfirmedFlushAsync(cancellationToken);
            if (flush is not { } slotLsn)
            {
                throw new StartupCheckException(
                    "Слот исчез при согласовании — confirmed_flush недоступен для принятия позиции слота как checkpoint.");
            }

            await checkpointStore.SaveTransactionAsync(slotLsn, cancellationToken);
            metrics.RegisterCheckpointSkip();
            logger.LogWarning(
                "Checkpoint {CheckpointLsn} позади confirmed_flush слота на {DiffBytes} байт — позиция слота {FlushLsn} " +
                "принята как checkpoint до начала чтения; события в окне между позициями пропускаются, их доставка " +
                "недоказуема (состояние недостижимо штатной работой сервиса — проверь внешние действия со слотом).",
                LsnText.Format(saved),
                diff,
                LsnText.Format(slotLsn));
        }
    }
}
