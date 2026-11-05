namespace CdcOutbox.Core.Processing;

/// <summary>
/// Терминальное нарушение стартовых проверок: повторные попытки бессмысленны,
/// необходимо вмешательство оператора.
/// </summary>
public sealed class StartupCheckException(string message, Exception? innerException = null)
    : InvalidOperationException(message, innerException);
