using McStats.Api.Configuration;
using McStats.Api.Middleware;
using McStats.Api.Models;
using McStats.Api.Services;
using McStats.Api.Storage;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Options;
using System.Threading.RateLimiting;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration
    .AddJsonFile("mcstats.config.json", optional: false, reloadOnChange: true);

builder.Services
    .AddOptions<McStatsOptions>()
    .Bind(builder.Configuration.GetSection("McStats"))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<BlockedUuidProvider>();
builder.Services.AddSingleton<StatsRepository>();
builder.Services.AddSingleton<StatsService>();
builder.Services.AddSingleton<AnalyticsService>();
builder.Services.AddSingleton<TelemetryService>();
builder.Services.AddSingleton<IngestRequestAuthenticator>();
builder.Services.AddSingleton<IngestIdempotencyService>();
builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    options.AddPolicy("ingest", _ => RateLimitPartition.GetFixedWindowLimiter("ingest", _ => new FixedWindowRateLimiterOptions
    {
        PermitLimit = 300,
        Window = TimeSpan.FromMinutes(1),
        QueueLimit = 0,
        QueueProcessingOrder = QueueProcessingOrder.OldestFirst
    }));
    options.AddPolicy("public", _ => RateLimitPartition.GetFixedWindowLimiter("public", _ => new FixedWindowRateLimiterOptions
    {
        PermitLimit = 1200,
        Window = TimeSpan.FromMinutes(1),
        QueueLimit = 0,
        QueueProcessingOrder = QueueProcessingOrder.OldestFirst
    }));
});

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();
app.UseRateLimiter();
app.UseMiddleware<IngestSecurityMiddleware>();

using (var scope = app.Services.CreateScope())
{
    var repository = scope.ServiceProvider.GetRequiredService<StatsRepository>();
    await repository.InitializeAsync();
}

app.MapGet("/healthz", async (StatsRepository repository, CancellationToken ct) =>
{
    var healthy = await repository.PingAsync(ct);
    return healthy
        ? Results.Ok(new { status = "ok" })
        : Results.Problem("Database is unavailable", statusCode: StatusCodes.Status503ServiceUnavailable);
}).RequireRateLimiting("public");

app.MapPost("/v1/events/batch", async (
    BatchIngestRequest request,
    IOptionsMonitor<McStatsOptions> options,
    StatsService service,
    HttpContext httpContext,
    CancellationToken ct) =>
{
    if (httpContext.Items.ContainsKey("ingest.replay"))
    {
        return Results.Ok(new { ingested = 0, replayed = true });
    }

    if (request.Events.Count == 0)
    {
        return Results.BadRequest(new { error = "events must not be empty" });
    }

    if (request.Events.Count > options.CurrentValue.MaxEventBatchSize)
    {
        return Results.BadRequest(new { error = $"events exceeds max batch size {options.CurrentValue.MaxEventBatchSize}" });
    }

    var ingested = await service.IngestAsync(request.Events, ct);
    return Results.Ok(new { ingested });
})
.WithMetadata(new RequestSizeLimitAttribute(1024 * 1024))
.RequireRateLimiting("ingest");

app.MapPost("/v1/telemetry/batch", async (
    TelemetryBatchRequest request,
    IOptionsMonitor<McStatsOptions> options,
    TelemetryService telemetryService,
    HttpContext httpContext,
    CancellationToken ct) =>
{
    if (httpContext.Items.ContainsKey("ingest.replay"))
    {
        return Results.Ok(new { ingested = 0, replayed = true });
    }

    if (request.Samples.Count == 0)
    {
        return Results.BadRequest(new { error = "samples must not be empty" });
    }

    if (request.Samples.Count > options.CurrentValue.MaxTelemetryBatchSize)
    {
        return Results.BadRequest(new { error = $"samples exceeds max batch size {options.CurrentValue.MaxTelemetryBatchSize}" });
    }

    var ingested = await telemetryService.IngestAsync(request.Samples, ct);
    return Results.Ok(new { ingested });
})
.WithMetadata(new RequestSizeLimitAttribute(1024 * 1024))
.RequireRateLimiting("ingest");

app.MapGet("/v1/stats/{uuid:guid}/{metric}", async (
    Guid uuid,
    string metric,
    string period,
    StatsService service,
    BlockedUuidProvider blockedUuidProvider,
    CancellationToken ct) =>
{
    if (blockedUuidProvider.IsBlocked(uuid))
    {
        return Results.StatusCode(StatusCodes.Status403Forbidden);
    }

    if (!PeriodExtensions.TryParse(period, out var parsedPeriod))
    {
        return Results.BadRequest(new { error = "invalid period. allowed: last1h,today,last7d,last30d,total" });
    }

    var result = await service.GetMetricStatAsync(uuid, metric, parsedPeriod, ct);
    return result is null
        ? Results.NotFound()
        : Results.Ok(result);
}).RequireRateLimiting("public");

app.MapGet("/v1/stats/{uuid:guid}", async (
    Guid uuid,
    string period,
    StatsService service,
    BlockedUuidProvider blockedUuidProvider,
    CancellationToken ct) =>
{
    if (blockedUuidProvider.IsBlocked(uuid))
    {
        return Results.StatusCode(StatusCodes.Status403Forbidden);
    }

    if (!PeriodExtensions.TryParse(period, out var parsedPeriod))
    {
        return Results.BadRequest(new { error = "invalid period. allowed: last1h,today,last7d,last30d,total" });
    }

    var result = await service.GetAllMetricStatsAsync(uuid, parsedPeriod, ct);
    return Results.Ok(result);
}).RequireRateLimiting("public");

app.MapGet("/v1/config/blocked-uuids", (BlockedUuidProvider blockedUuidProvider) =>
{
    return Results.Ok(new { blocked = blockedUuidProvider.All().Select(x => x.ToString()) });
}).RequireRateLimiting("public");

app.MapGet("/v1/analytics/overview", async (
    int? days,
    AnalyticsService analyticsService,
    BlockedUuidProvider blockedUuidProvider,
    CancellationToken ct) =>
{
    var requestedDays = days ?? 30;
    if (requestedDays is < 1 or > 365)
    {
        return Results.BadRequest(new { error = "days must be between 1 and 365" });
    }

    var result = await analyticsService.GetOverviewAsync(requestedDays, blockedUuidProvider.All(), ct);
    return Results.Ok(result);
}).RequireRateLimiting("public");

app.MapGet("/v1/analytics/daily", async (
    int? days,
    AnalyticsService analyticsService,
    BlockedUuidProvider blockedUuidProvider,
    CancellationToken ct) =>
{
    var requestedDays = days ?? 30;
    if (requestedDays is < 1 or > 365)
    {
        return Results.BadRequest(new { error = "days must be between 1 and 365" });
    }

    var result = await analyticsService.GetDailyAnalyticsAsync(requestedDays, blockedUuidProvider.All(), ct);
    return Results.Ok(result);
}).RequireRateLimiting("public");

app.MapGet("/v1/telemetry/overview", async (
    string serverId,
    int? hours,
    TelemetryService telemetryService,
    CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(serverId))
    {
        return Results.BadRequest(new { error = "serverId is required" });
    }

    var requestedHours = hours ?? 24;
    if (requestedHours is < 1 or > 24 * 30)
    {
        return Results.BadRequest(new { error = "hours must be between 1 and 720" });
    }

    var result = await telemetryService.GetOverviewAsync(serverId.Trim(), requestedHours, ct);
    return Results.Ok(result);
}).RequireRateLimiting("public");

app.MapGet("/v1/telemetry/series", async (
    string serverId,
    int? hours,
    string? resolution,
    TelemetryService telemetryService,
    CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(serverId))
    {
        return Results.BadRequest(new { error = "serverId is required" });
    }

    var requestedHours = hours ?? 24;
    if (requestedHours is < 1 or > 24 * 30)
    {
        return Results.BadRequest(new { error = "hours must be between 1 and 720" });
    }

    var resolved = string.IsNullOrWhiteSpace(resolution) ? "hour" : resolution.Trim().ToLowerInvariant();
    if (resolved is not ("minute" or "hour"))
    {
        return Results.BadRequest(new { error = "resolution must be minute or hour" });
    }

    var result = await telemetryService.GetSeriesAsync(serverId.Trim(), requestedHours, resolved, ct);
    return Results.Ok(result);
}).RequireRateLimiting("public");

app.MapGet("/v1/telemetry/servers", async (
    TelemetryService telemetryService,
    CancellationToken ct) =>
{
    var servers = await telemetryService.GetServerIdsAsync(ct);
    return Results.Ok(new { servers });
}).RequireRateLimiting("public");

app.Run();

public partial class Program;
