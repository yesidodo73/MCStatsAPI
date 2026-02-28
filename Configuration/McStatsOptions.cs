using System.ComponentModel.DataAnnotations;

namespace McStats.Api.Configuration;

public sealed class McStatsOptions
{
    [Required]
    public required string DatabasePath { get; init; }

    [Required]
    public required string TimeZoneId { get; init; }

    public HashSet<string> BlockedUuids { get; init; } = [];

    public int MaxEventBatchSize { get; init; } = 500;

    public int MaxTelemetryBatchSize { get; init; } = 300;

    public int MaxMetricLength { get; init; } = 64;

    public int MaxClockSkewSeconds { get; init; } = 300;

    public IngestSecurityOptions Security { get; init; } = new();
}

public sealed class IngestSecurityOptions
{
    public List<IngestClientCredential> Clients { get; init; } = [];
}

public sealed class IngestClientCredential
{
    [Required]
    public required string ServerId { get; init; }

    [Required]
    public required string ApiKey { get; init; }

    [Required]
    public required string Secret { get; init; }
}
