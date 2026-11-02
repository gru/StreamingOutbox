using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CdcOutbox.Core.Processing;

/// <summary>
/// Стартовый блок проверки объектов доставки: checkpoint-topic, согласование
/// сохранённого checkpoint со слотом, обеспечение failover-слота, проверка
/// publication; любая ошибка останавливает запуск хоста.
/// </summary>
public sealed class OutboxStartupChecks(
    OutboxSlotProvisioner slotProvisioner,
    PublicationVerifier publicationVerifier,
    CheckpointTopicVerifier checkpointTopicVerifier,
    CheckpointStore checkpointStore,
    ILogger<OutboxStartupChecks> logger) : IHostedService
{

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        checkpointTopicVerifier.Verify();
        await ReconcileCheckpointWithSlotAsync(cancellationToken);
        await slotProvisioner.EnsureAsync(cancellationToken);
        await publicationVerifier.VerifyAsync(cancellationToken);
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
