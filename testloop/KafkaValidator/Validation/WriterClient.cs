using System.Text.Json;

using KafkaValidator.Contracts;

namespace KafkaValidator.Validation;

/// <summary>
/// Клиент Writer: ожидаемый набор id последнего завершённого прогона
/// (GET /api/generation/results/ids — RunIdsDto создан для KafkaValidator).
/// 404 = завершённого прогона нет.
/// </summary>
public sealed class WriterClient(HttpClient http)
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    /// <summary>
    /// Ожидаемый набор id последнего завершённого прогона Writer;
    /// null — завершённого прогона нет (404).
    /// </summary>
    /// <exception cref="HttpRequestException">
    /// Writer недоступен или ответил не 200/404.
    /// </exception>
    public async Task<WriterRunIds?> FetchLastRunIdsAsync(CancellationToken ct)
    {
        using var response = await http.GetAsync("/api/generation/results/ids", ct);
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            return null;
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<WriterRunIds>(JsonOptions, ct);
    }
}
