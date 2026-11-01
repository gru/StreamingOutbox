using System.Text;
using Prometheus;

namespace CdcOutbox.UnitTests;

internal static class MetricsExport
{
    public static async Task<string> ExportAsync(CollectorRegistry registry)
    {
        using var stream = new MemoryStream();
        await registry.CollectAndExportAsTextAsync(stream);
        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
