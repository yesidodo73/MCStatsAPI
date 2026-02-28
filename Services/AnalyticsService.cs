using McStats.Api.Configuration;
using McStats.Api.Models;
using McStats.Api.Storage;
using Microsoft.Extensions.Options;

namespace McStats.Api.Services;

public sealed class AnalyticsService
{
    private const int DaySeconds = 86400;
    private const int HourSeconds = 3600;

    private readonly StatsRepository _repository;
    private readonly BucketCalculator _bucketCalculator;

    public AnalyticsService(StatsRepository repository, IOptionsMonitor<McStatsOptions> options)
    {
        _repository = repository;
        var timeZone = TimeZoneInfo.FindSystemTimeZoneById(options.CurrentValue.TimeZoneId);
        _bucketCalculator = new BucketCalculator(timeZone);
    }

    public async Task<AnalyticsOverviewDto> GetOverviewAsync(int days, IReadOnlyCollection<Guid> blockedUuids, CancellationToken ct)
    {
        var nowUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var currentEndDayBucket = _bucketCalculator.CurrentDayStartUnix(nowUnix);
        var currentStartDayBucket = currentEndDayBucket - ((days - 1L) * DaySeconds);

        var previousEndDayBucket = currentStartDayBucket - DaySeconds;
        var previousStartDayBucket = previousEndDayBucket - ((days - 1L) * DaySeconds);

        var activePlayers = await _repository.QueryActivePlayersCountAsync(currentStartDayBucket, currentEndDayBucket, blockedUuids, ct);
        var previousActivePlayers = await _repository.QueryActivePlayersCountAsync(previousStartDayBucket, previousEndDayBucket, blockedUuids, ct);

        var newPlayers = await _repository.QueryNewPlayersCountAsync(currentStartDayBucket, currentEndDayBucket, blockedUuids, ct);
        var previousNewPlayers = await _repository.QueryNewPlayersCountAsync(previousStartDayBucket, previousEndDayBucket, blockedUuids, ct);

        var currentDailyActive = await _repository.QueryDailyActivePlayersAsync(currentStartDayBucket, currentEndDayBucket, blockedUuids, ct);
        var previousDailyActive = await _repository.QueryDailyActivePlayersAsync(previousStartDayBucket, previousEndDayBucket, blockedUuids, ct);

        var averageDailyActivePlayers = CalculateAverage(currentDailyActive, currentStartDayBucket, currentEndDayBucket);
        var previousAverageDailyActivePlayers = CalculateAverage(previousDailyActive, previousStartDayBucket, previousEndDayBucket);

        var startHourBucket = _bucketCalculator.ToHourBucketUnix(currentStartDayBucket);
        var endHourBucket = _bucketCalculator.ToHourBucketUnix(nowUnix);
        var hourly = await _repository.QueryHourlyActivePlayersAsync(startHourBucket, endHourBucket, blockedUuids, ct);
        var hourlyAverages = BuildHourlyAverages(hourly);

        return new AnalyticsOverviewDto(
            days,
            _bucketCalculator.ToLocalDateString(currentStartDayBucket),
            _bucketCalculator.ToLocalDateString(currentEndDayBucket),
            newPlayers,
            activePlayers,
            Math.Round(averageDailyActivePlayers, 2),
            hourlyAverages,
            BuildTrend(newPlayers, previousNewPlayers),
            BuildTrend(activePlayers, previousActivePlayers),
            BuildTrend(
                averageDailyActivePlayers,
                previousAverageDailyActivePlayers)
        );
    }

    public async Task<DailyAnalyticsResponse> GetDailyAnalyticsAsync(int days, IReadOnlyCollection<Guid> blockedUuids, CancellationToken ct)
    {
        var nowUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var endDayBucket = _bucketCalculator.CurrentDayStartUnix(nowUnix);
        var startDayBucket = endDayBucket - ((days - 1L) * DaySeconds);

        var activeSeries = await _repository.QueryDailyActivePlayersAsync(startDayBucket, endDayBucket, blockedUuids, ct);
        var newSeries = await _repository.QueryDailyNewPlayersAsync(startDayBucket, endDayBucket, blockedUuids, ct);

        var points = new List<DailyAnalyticsPointDto>(days);
        for (var dayBucket = startDayBucket; dayBucket <= endDayBucket; dayBucket += DaySeconds)
        {
            activeSeries.TryGetValue(dayBucket, out var activePlayers);
            newSeries.TryGetValue(dayBucket, out var newPlayers);

            points.Add(new DailyAnalyticsPointDto(
                _bucketCalculator.ToLocalDateString(dayBucket),
                activePlayers,
                newPlayers));
        }

        return new DailyAnalyticsResponse(
            days,
            _bucketCalculator.ToLocalDateString(startDayBucket),
            _bucketCalculator.ToLocalDateString(endDayBucket),
            points);
    }

    private List<HourlyAveragePlayersDto> BuildHourlyAverages(IReadOnlyDictionary<long, int> hourlyCounts)
    {
        var sums = new double[24];
        var buckets = new int[24];

        foreach (var (hourBucket, players) in hourlyCounts)
        {
            var localHour = _bucketCalculator.ToLocalTime(hourBucket).Hour;
            sums[localHour] += players;
            buckets[localHour] += 1;
        }

        var result = new List<HourlyAveragePlayersDto>(24);
        for (var hour = 0; hour < 24; hour++)
        {
            var avg = buckets[hour] == 0 ? 0 : sums[hour] / buckets[hour];
            result.Add(new HourlyAveragePlayersDto(hour, Math.Round(avg, 2)));
        }

        return result;
    }

    private static double CalculateAverage(IReadOnlyDictionary<long, int> series, long startDayBucket, long endDayBucket)
    {
        var totalDays = ((endDayBucket - startDayBucket) / DaySeconds) + 1;
        if (totalDays <= 0)
        {
            return 0;
        }

        long sum = 0;
        for (var dayBucket = startDayBucket; dayBucket <= endDayBucket; dayBucket += DaySeconds)
        {
            series.TryGetValue(dayBucket, out var value);
            sum += value;
        }

        return sum / (double)totalDays;
    }

    private static TrendMetricDto BuildTrend(double current, double previous)
    {
        var delta = current - previous;
        var direction = delta switch
        {
            > 0 => "up",
            < 0 => "down",
            _ => "flat"
        };

        var changePercent = previous == 0
            ? (current == 0 ? 0 : 100)
            : Math.Round((delta / (double)previous) * 100, 2);

        return new TrendMetricDto(
            Math.Round(current, 2),
            Math.Round(previous, 2),
            Math.Round(delta, 2),
            changePercent,
            direction);
    }
}
