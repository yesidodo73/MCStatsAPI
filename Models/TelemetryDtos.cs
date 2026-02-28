namespace McStats.Api.Models;

public sealed record TelemetrySampleDto(
    string ServerId,
    long? TimestampUtc,
    double? Tps,
    double? Mspt,
    double? CpuUsagePercent,
    double? RamUsedMb,
    double? RamTotalMb,
    double? NetworkRxKbps,
    double? NetworkTxKbps,
    int? OnlinePlayers,
    double? PingP50Ms,
    double? PingP95Ms,
    double? PingP99Ms
);

public sealed record TelemetryBatchRequest(
    IReadOnlyList<TelemetrySampleDto> Samples
);

public sealed record TelemetryOverviewDto(
    string ServerId,
    int Hours,
    string WindowStart,
    string WindowEnd,
    double AvgTps,
    double AvgMspt,
    double AvgCpuUsagePercent,
    double AvgRamUsedMb,
    double AvgNetworkRxKbps,
    double AvgNetworkTxKbps,
    double AvgOnlinePlayers
);

public sealed record TelemetryPointDto(
    long BucketUtc,
    double AvgTps,
    double AvgMspt,
    double AvgCpuUsagePercent,
    double AvgRamUsedMb,
    double AvgNetworkRxKbps,
    double AvgNetworkTxKbps,
    double AvgOnlinePlayers
);

public sealed record TelemetrySeriesResponse(
    string ServerId,
    int Hours,
    string Resolution,
    IReadOnlyList<TelemetryPointDto> Points
);
