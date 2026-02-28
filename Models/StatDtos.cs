namespace McStats.Api.Models;

public sealed record IngestEventDto(
    Guid Uuid,
    string Metric,
    long Delta,
    long? TimestampUtc = null
);

public sealed record BatchIngestRequest(
    IReadOnlyList<IngestEventDto> Events
);

public sealed record MetricStatDto(
    Guid Uuid,
    string Metric,
    string Period,
    long Value
);

public sealed record PlayerStatsResponse(
    Guid Uuid,
    string Period,
    IReadOnlyDictionary<string, long> Metrics
);
