using McStats.Api.Configuration;
using McStats.Api.Models;
using McStats.Api.Storage;
using Microsoft.Extensions.Options;

namespace McStats.Api.Services;

public sealed class StatsService
{
    private readonly StatsRepository _repository;
    private readonly BucketCalculator _bucketCalculator;

    public StatsService(StatsRepository repository, IOptionsMonitor<McStatsOptions> options)
    {
        _repository = repository;
        var tz = TimeZoneInfo.FindSystemTimeZoneById(options.CurrentValue.TimeZoneId);
        _bucketCalculator = new BucketCalculator(tz);
    }

    public async Task<int> IngestAsync(IReadOnlyList<IngestEventDto> events, CancellationToken ct)
    {
        var prepared = new List<PreparedEvent>(events.Count);
        foreach (var item in events)
        {
            if (item.Delta == 0 || string.IsNullOrWhiteSpace(item.Metric))
            {
                continue;
            }

            var ts = item.TimestampUtc ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            prepared.Add(new PreparedEvent(
                item.Uuid,
                item.Metric.Trim(),
                item.Delta,
                ts,
                _bucketCalculator.ToMinuteBucketUnix(ts),
                _bucketCalculator.ToHourBucketUnix(ts),
                _bucketCalculator.ToDayBucketUnix(ts)
            ));
        }

        if (prepared.Count == 0)
        {
            return 0;
        }

        await _repository.IngestBatchAsync(prepared, ct);
        return prepared.Count;
    }

    public async Task<MetricStatDto?> GetMetricStatAsync(Guid uuid, string metric, Period period, CancellationToken ct)
    {
        var normalizedMetric = metric.Trim();
        if (string.IsNullOrEmpty(normalizedMetric))
        {
            return null;
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var value = await _repository.QueryMetricAsync(uuid, normalizedMetric, period, BuildPeriodStart(period, now), ct);
        if (value is null)
        {
            return null;
        }

        return new MetricStatDto(uuid, normalizedMetric, period.ToApiString(), value.Value);
    }

    public async Task<PlayerStatsResponse> GetAllMetricStatsAsync(Guid uuid, Period period, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var metrics = await _repository.QueryAllMetricsAsync(uuid, period, BuildPeriodStart(period, now), ct);
        return new PlayerStatsResponse(uuid, period.ToApiString(), metrics);
    }

    private long BuildPeriodStart(Period period, long nowUnix)
    {
        return period switch
        {
            Period.Last1H => nowUnix - 3600,
            Period.Today => _bucketCalculator.CurrentDayStartUnix(nowUnix),
            Period.Last7D => _bucketCalculator.CurrentDayStartUnix(nowUnix) - (6 * 86400),
            Period.Last30D => _bucketCalculator.CurrentDayStartUnix(nowUnix) - (29 * 86400),
            Period.Total => 0,
            _ => 0
        };
    }
}

public sealed record PreparedEvent(
    Guid Uuid,
    string Metric,
    long Delta,
    long EventTs,
    long MinuteBucket,
    long HourBucket,
    long DayBucket
);
