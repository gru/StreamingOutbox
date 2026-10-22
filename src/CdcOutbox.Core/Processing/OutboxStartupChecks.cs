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
public sealed class OutboxStartupChecks : IHostedService
{
    private readonly OutboxSlotProvisioner _slotProvisioner;
    private readonly PublicationVerifier _publicationVerifier;
    private readonly CheckpointTopicVerifier _checkpointTopicVerifier;

    /// <summary>
    /// Создать стартовый блок проверки.
    /// </summary>
    /// <param name="slotProvisioner">Обеспечение failover-слота.</param>
    /// <param name="publicationVerifier">Проверка publication.</param>
    /// <param name="checkpointTopicVerifier">Проверка checkpoint-topic.</param>
    public OutboxStartupChecks(
        OutboxSlotProvisioner slotProvisioner,
        PublicationVerifier publicationVerifier,
        CheckpointTopicVerifier checkpointTopicVerifier)
    {
        _slotProvisioner = slotProvisioner;
        _publicationVerifier = publicationVerifier;
        _checkpointTopicVerifier = checkpointTopicVerifier;
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _slotProvisioner.EnsureAsync(cancellationToken);
        await _publicationVerifier.VerifyAsync(cancellationToken);
        _checkpointTopicVerifier.Verify();
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
