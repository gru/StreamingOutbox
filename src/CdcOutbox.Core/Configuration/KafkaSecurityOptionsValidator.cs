using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Валидатор согласованности параметров безопасности Kafka при старте сервиса.
/// </summary>
public sealed class KafkaSecurityOptionsValidator : IValidateOptions<KafkaOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, KafkaOptions options)
    {
        var issue = KafkaOptions.FindSecurityIssue(options);
        return issue is null
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(issue);
    }
}
