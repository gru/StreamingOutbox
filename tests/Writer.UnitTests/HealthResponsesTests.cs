using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Shouldly;
using Writer.Health;
using Xunit;

namespace Writer.UnitTests;

public class HealthResponsesTests
{
    private static async Task<(string Body, string? ContentType)> WriteAsync(HealthReport report)
    {
        var context = new DefaultHttpContext();
        context.Response.Body = new MemoryStream();
        await HealthResponses.WriteAsync(context, report);
        context.Response.Body.Position = 0;
        using var reader = new StreamReader(context.Response.Body);
        return (await reader.ReadToEndAsync(), context.Response.ContentType);
    }

    [Fact]
    public async Task Write_Healthy_OnlyStatus()
    {
        var report = new HealthReport(
            new Dictionary<string, HealthReportEntry> { ["postgres"] = new(HealthStatus.Healthy, "ok", TimeSpan.Zero, null, null) },
            TimeSpan.Zero);

        var (body, contentType) = await WriteAsync(report);

        body.ShouldBe("{\"status\":\"Healthy\"}");
        contentType.ShouldBe("application/json; charset=utf-8");
    }

    [Fact]
    public async Task Write_Unhealthy_StatusAndDescription()
    {
        var report = new HealthReport(
            new Dictionary<string, HealthReportEntry>
            {
                ["postgres"] = new(HealthStatus.Unhealthy, "connection refused", TimeSpan.Zero, null, null),
            },
            TimeSpan.Zero);

        var (body, _) = await WriteAsync(report);

        using var doc = JsonDocument.Parse(body);
        doc.RootElement.GetProperty("status").GetString().ShouldBe("Unhealthy");
        doc.RootElement.GetProperty("description").GetString().ShouldNotBeNull().ShouldContain("postgres");
        doc.RootElement.GetProperty("description").GetString().ShouldNotBeNull().ShouldContain("connection refused");
    }

    [Fact]
    public async Task Write_EmptyReport_Liveness_Healthy()
    {
        // /health мапится с Predicate = (_ => false): чеков нет, отчёт пустой
        var report = new HealthReport(
            new Dictionary<string, HealthReportEntry>(),
            TimeSpan.Zero);

        var (body, _) = await WriteAsync(report);

        body.ShouldBe("{\"status\":\"Healthy\"}");
    }
}
