using McStats.Api.Configuration;
using McStats.Api.Models;
using McStats.Api.Storage;
using Microsoft.Extensions.Options;

namespace McStats.Api.Services;

public sealed class TelemetryService
{
    private readonly StatsRepository _repository;
    private readonly BucketCalculator _bucketCalculator;
    private readonly int _maxTelemetryBatchSize;

    public TelemetryService(StatsRepository repository, IOptionsMonitor<McStatsOptions> options)
    {
        _repository = repository;
        var currentOptions = options.CurrentValue;
        var timeZone = TimeZoneInfo.FindSystemTimeZoneById(currentOptions.TimeZoneId);
        _bucketCalculator = new BucketCalculator(timeZone);
        _maxTelemetryBatchSize = Math.Max(1, currentOptions.MaxTelemetryBatchSize);
    }

    public async Task<int> IngestAsync(IReadOnlyList<TelemetrySampleDto> samples, CancellationToken ct)
    {
        var prepared = new List<PreparedTelemetrySample>(Math.Min(samples.Count, _maxTelemetryBatchSize));
        foreach (var sample in samples.Take(_maxTelemetryBatchSize))
        {
            if (string.IsNullOrWhiteSpace(sample.ServerId))
            {
                continue;
            }

            var ts = sample.TimestampUtc ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            if (ts < 0)
            {
                continue;
            }
            prepared.Add(new PreparedTelemetrySample(
                sample.ServerId.Trim(),
                ts,
                _bucketCalculator.ToMinuteBucketUnix(ts),
                _bucketCalculator.ToHourBucketUnix(ts),
                sample.Tps,
                sample.Mspt,
                sample.CpuUsagePercent,
                sample.RamUsedMb,
                sample.RamTotalMb,
                sample.NetworkRxKbps,
                sample.NetworkTxKbps,
                sample.DiskReadKbps,
                sample.DiskWriteKbps,
                sample.GcCollectionsPerMinute,
                sample.ThreadCount,
                sample.OnlinePlayers,
                sample.PingP50Ms,
                sample.PingP95Ms,
                sample.PingP99Ms
            ));
        }

        if (prepared.Count == 0)
        {
            return 0;
        }

        await _repository.IngestTelemetryBatchAsync(prepared, ct);
        return prepared.Count;
    }

    public async Task<TelemetryOverviewDto> GetOverviewAsync(string serverId, int hours, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var start = now - (hours * 3600L);
        var aggregated = await _repository.QueryTelemetryOverviewAsync(serverId, start, now, ct);
        return new TelemetryOverviewDto(
            serverId,
            hours,
            DateTimeOffset.FromUnixTimeSeconds(start).ToString("O"),
            DateTimeOffset.FromUnixTimeSeconds(now).ToString("O"),
            aggregated.AvgTps,
            aggregated.AvgMspt,
            aggregated.AvgCpuUsagePercent,
            aggregated.AvgRamUsedMb,
            aggregated.AvgNetworkRxKbps,
            aggregated.AvgNetworkTxKbps,
            aggregated.AvgDiskReadKbps,
            aggregated.AvgDiskWriteKbps,
            aggregated.AvgGcCollectionsPerMinute,
            aggregated.AvgThreadCount,
            aggregated.AvgOnlinePlayers
        );
    }

    public async Task<TelemetrySeriesResponse> GetSeriesAsync(string serverId, int hours, string resolution, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var start = now - (hours * 3600L);

        IReadOnlyList<TelemetryPointDto> points = resolution switch
        {
            "minute" => await _repository.QueryTelemetrySeriesByMinuteAsync(serverId, start, now, ct),
            "hour" => await _repository.QueryTelemetrySeriesByHourAsync(serverId, start, now, ct),
            _ => throw new ArgumentException("invalid resolution. allowed: minute,hour")
        };

        return new TelemetrySeriesResponse(serverId, hours, resolution, points);
    }

    public async Task<IReadOnlyList<string>> GetServerIdsAsync(CancellationToken ct)
    {
        return await _repository.QueryTelemetryServerIdsAsync(ct);
    }
}

public sealed record PreparedTelemetrySample(
    string ServerId,
    long SampleTs,
    long MinuteBucket,
    long HourBucket,
    double? Tps,
    double? Mspt,
    double? CpuUsagePercent,
    double? RamUsedMb,
    double? RamTotalMb,
    double? NetworkRxKbps,
    double? NetworkTxKbps,
    double? DiskReadKbps,
    double? DiskWriteKbps,
    double? GcCollectionsPerMinute,
    int? ThreadCount,
    int? OnlinePlayers,
    double? PingP50Ms,
    double? PingP95Ms,
    double? PingP99Ms
);
