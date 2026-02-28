using System.ComponentModel.DataAnnotations;

namespace McStats.Api.Configuration;

public sealed class McStatsOptions
{
    [Required]
    public required string DatabasePath { get; init; }

    [Required]
    public required string TimeZoneId { get; init; }

    public HashSet<string> BlockedUuids { get; init; } = [];
}
