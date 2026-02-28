using McStats.Api.Storage;

namespace McStats.Api.Services;

public sealed class IngestIdempotencyService
{
    private readonly StatsRepository _repository;

    public IngestIdempotencyService(StatsRepository repository)
    {
        _repository = repository;
    }

    public async Task<bool> TryRegisterAsync(string endpointName, string serverId, string idempotencyKey, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var expiresAt = now + 24 * 3600;
        return await _repository.TryRegisterIngestIdempotencyKeyAsync(endpointName, serverId, idempotencyKey, now, expiresAt, ct);
    }
}
