using Microsoft.Extensions.Options;

namespace CdcOutbox.Core.Configuration;

/// <summary>
/// Валидирует комбинацию параметров <see cref="OutboxStorageOptions"/>.
/// </summary>
public sealed class OutboxStorageOptionsValidator : IValidateOptions<OutboxStorageOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, OutboxStorageOptions options)
    {
        if (options.UsesWalMessages && string.IsNullOrWhiteSpace(options.WalMessagePrefix))
        {
            return ValidateOptionsResult.Fail(
                "CdcOutbox:Storage:WalMessagePrefix не задан при включённом чтении WAL-сообщений.");
        }

        if (options.UsesTable && !IsSafeIdentifier(options.Schema))
        {
            return ValidateOptionsResult.Fail(
                $"CdcOutbox:Storage:Schema содержит недопустимый SQL-идентификатор: '{options.Schema}'.");
        }

        if (options.UsesTable && !IsSafeIdentifier(options.Table))
        {
            return ValidateOptionsResult.Fail(
                $"CdcOutbox:Storage:Table содержит недопустимый SQL-идентификатор: '{options.Table}'.");
        }

        return ValidateOptionsResult.Success;
    }

    internal static bool IsSafeIdentifier(string? identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            return false;
        }

        var trimmed = identifier.Trim();
        if (trimmed.Length > 63)
        {
            return false;
        }

        if (!(char.IsAsciiLetter(trimmed[0]) || trimmed[0] == '_'))
        {
            return false;
        }

        for (var i = 1; i < trimmed.Length; i++)
        {
            var c = trimmed[i];
            if (!(char.IsAsciiLetterOrDigit(c) || c == '_'))
            {
                return false;
            }
        }

        return true;
    }
}
