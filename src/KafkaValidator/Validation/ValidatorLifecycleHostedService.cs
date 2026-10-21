namespace KafkaValidator.Validation;

/// <summary>
/// running выставляется ПОСЛЕ старта hosted-сервиса KafkaFlow (hosted-сервисы
/// стартуют в порядке регистрации — этот регистрируется позже). Готовность не
/// зависит от наличия записей: на пустом topic (свежий стенд) валидатор тоже
/// готов — просто ещё нечего сверять.
/// </summary>
public sealed class ValidatorLifecycleHostedService(ValidatorEngine engine) : IHostedService
{
    /// <summary>
    /// Шина (и консюмер) уже запущены — перевести сервис в running.
    /// </summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        engine.MarkRunning();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Штатная остановка — перевести сервис в stopped.
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        engine.MarkStopped();
        return Task.CompletedTask;
    }
}
