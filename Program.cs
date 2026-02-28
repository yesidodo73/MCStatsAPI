using McStats.Api.Configuration;
using McStats.Api.Models;
using McStats.Api.Services;
using McStats.Api.Storage;

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

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

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
});

app.MapPost("/v1/events/batch", async (
    BatchIngestRequest request,
    StatsService service,
    CancellationToken ct) =>
{
    if (request.Events.Count == 0)
    {
        return Results.BadRequest(new { error = "events must not be empty" });
    }

    var ingested = await service.IngestAsync(request.Events, ct);
    return Results.Ok(new { ingested });
});

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
});

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
});

app.MapGet("/v1/config/blocked-uuids", (BlockedUuidProvider blockedUuidProvider) =>
{
    return Results.Ok(new { blocked = blockedUuidProvider.All().Select(x => x.ToString()) });
});

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
});

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
});

app.Run();
