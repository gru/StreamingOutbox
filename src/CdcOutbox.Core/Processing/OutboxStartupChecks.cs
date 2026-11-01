using CdcOutbox.Core.Kafka;
using CdcOutbox.Core.Replication;
using Microsoft.Extensions.Hosting;

namespace CdcOutbox.Core.Processing;

/// <summary>
/// Стартовый блок проверки объектов доставки: обеспечение failover-слота,
/// проверка publication и checkpoint-topic. Запускается до <see cref="OutboxProcessor"/>
/// (порядок регистрации hosted-служб); любая ошибка останавливает запуск хоста.
/// Выделен в отдельную hosted-службу, а не встроен в <see cref="OutboxProcessor"/>,
/// чтобы процессор не тянул инфраструктурных зависимостей проверок и оставался
/// тестируемым без живых PostgreSQL и Kafka, а ошибка проверок останавливала
/// запуск хоста ещё до старта цикла процессора.
/// </summary>
public sealed class OutboxStartupChecks(
    OutboxSlotProvisioner slotProvisioner,
    PublicationVerifier publicationVerifier,
    CheckpointTopicVerifier checkpointTopicVerifier) : IHostedService
{

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await slotProvisioner.EnsureAsync(cancellationToken);
        await publicationVerifier.VerifyAsync(cancellationToken);
        checkpointTopicVerifier.Verify();
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
