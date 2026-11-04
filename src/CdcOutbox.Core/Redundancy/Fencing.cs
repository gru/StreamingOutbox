using Confluent.Kafka;

namespace CdcOutbox.Core.Redundancy;

/// <summary>
/// Распознавание fencing-ошибок продюсера — сигнал потери лидерства, а не сбой доставки.
/// </summary>
internal static class Fencing
{
    /// <summary>
    /// Проверить, является ли исключение fencing-ошибкой продюсера.
    /// </summary>
    /// <param name="exception">Исключение операций продюсера.</param>
    /// <returns>true, если продюсер заблокирован fencing.</returns>
    // в .NET-клиенте fencing приходит как KafkaException с кодом эпохи —
    // отдельного типа исключения нет
    internal static bool IsFenced(Exception exception) => exception switch
    {
        KafkaException kafkaException => kafkaException.Error.Code
            is ErrorCode.ProducerFenced
            or ErrorCode.InvalidProducerEpoch
            or ErrorCode.InvalidProducerIdMapping,
        _ => false,
    };
}
