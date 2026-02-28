using McStats.Api.Configuration;
using Microsoft.Extensions.Options;

namespace McStats.Api.Services;

public sealed class BlockedUuidProvider
{
    private readonly IOptionsMonitor<McStatsOptions> _options;

    public BlockedUuidProvider(IOptionsMonitor<McStatsOptions> options)
    {
        _options = options;
    }

    public bool IsBlocked(Guid uuid)
    {
        foreach (var value in _options.CurrentValue.BlockedUuids)
        {
            if (Guid.TryParse(value, out var parsed) && parsed == uuid)
            {
                return true;
            }
        }

        return false;
    }

    public IReadOnlyCollection<Guid> All()
    {
        var result = new List<Guid>();
        foreach (var value in _options.CurrentValue.BlockedUuids)
        {
            if (Guid.TryParse(value, out var parsed))
            {
                result.Add(parsed);
            }
        }

        return result;
    }
}
