using KafkaFlow;

namespace KafkaValidator.Kafka;

/// <summary>
/// Десериализатор-сквозняк: value приходит готовыми байтами body outbox-сообщения
/// (bytea pass-through) — отдаём их как byte[], разбор выполняет ValidationHandler
/// (OutboxMessageBody.TryParse).
/// </summary>
public sealed class PassthroughDeserializer : IDeserializer
{
    public async Task<object> DeserializeAsync(Stream input, Type type, ISerializerContext context)
    {
        using var buffer = new MemoryStream();
        await input.CopyToAsync(buffer);
        return buffer.ToArray();
    }
}
