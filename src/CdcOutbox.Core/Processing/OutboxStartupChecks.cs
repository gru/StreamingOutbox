using CdcOutbox.Core.Configuration;
using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Metrics;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Processing;

/// <summary>
/// Стартовый блок проверки объектов доставки: checkpoint-topic, согласование
/// сохранённого checkpoint со слотом (либо Recovery Mode — аварийное восстановление
/// с переписыванием checkpoint), обеспечение failover-слота, проверка
/// publication; любая ошибка останавливает запуск хоста.
/// </summary>
public sealed class OutboxStartupChecks(
    OutboxSlotProvisioner slotProvisioner,
    PublicationVerifier publicationVerifier,
    CheckpointTopicVerifier checkpointTopicVerifier,
    CheckpointStore checkpointStore,
    IOptions<CdcOutboxOptions> options,
    OutboxMetrics metrics,
    ILogger<OutboxStartupChecks> logger) : IHostedService
{
    private readonly CdcOutboxOptions _options = options.Value;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        checkpointTopicVerifier.Verify();
        // gauge выставляется на каждом старте: до вызова серия не экспортируется вовсе
        metrics.SetRecoveryMode(_options.RecoveryMode);
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

    private async Task RunRecoveryAsync(CancellationToken cancellationToken)
    {
        var checkpoint = checkpointStore.ReadLatest(cancellationToken);
        var slot = await slotProvisioner.FindSlotAsync(cancellationToken);
        var confirmedFlush = slot is null ? null : await slotProvisioner.GetConfirmedFlushAsync(cancellationToken);
        var decision = RecoveryPlanner.Plan(checkpoint, slot is not null, confirmedFlush);
        if (decision.Action == RecoveryAction.Reject)
        {
            throw new InvalidOperationException(decision.Description);
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
            throw new InvalidOperationException(
                "После обеспечения слота его confirmed_flush пуст — позиция для recovery-checkpoint недоступна.");
        }

        // новый checkpoint сохраняется до начала чтения: последующие перезапуски
        // проходят штатное согласование уже без recovery
        await checkpointStore.SaveAsync(target, cancellationToken);
        logger.LogWarning(
            "Recovery Mode: checkpoint {OldLsn} → {NewLsn}; события в окне между позициями пропускаются — " +
            "осознанный отказ от их доставки по решению оператора. После восстановления перезапусти сервис без флага.",
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
            throw new InvalidOperationException(issue);
        }

        logger.LogInformation(
            "Checkpoint {CheckpointLsn}, confirmed_flush слота опережает на {DiffBytes} байт — продолжение с позиции слота.",
            LsnText.Format(saved),
            diff);
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
