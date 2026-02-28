using McStats.Api.Configuration;
using Microsoft.Extensions.Options;
using System.Security.Cryptography;
using System.Text;

namespace McStats.Api.Services;

public sealed class IngestRequestAuthenticator
{
    private readonly IOptionsMonitor<McStatsOptions> _options;

    public IngestRequestAuthenticator(IOptionsMonitor<McStatsOptions> options)
    {
        _options = options;
    }

    public bool TryValidate(
        string serverId,
        string apiKey,
        string timestamp,
        string signature,
        string rawBody,
        out string error)
    {
        error = string.Empty;
        var options = _options.CurrentValue;

        var credential = options.Security.Clients.FirstOrDefault(c =>
            string.Equals(c.ServerId, serverId, StringComparison.Ordinal) &&
            string.Equals(c.ApiKey, apiKey, StringComparison.Ordinal));
        if (credential is null)
        {
            error = "unknown server or api key";
            return false;
        }

        if (!long.TryParse(timestamp, out var ts))
        {
            error = "invalid timestamp";
            return false;
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        if (Math.Abs(now - ts) > options.MaxClockSkewSeconds)
        {
            error = "timestamp out of allowed clock skew";
            return false;
        }

        var payload = $"{timestamp}\n{rawBody}";
        var expected = ComputeHexHmacSha256(payload, credential.Secret);
        if (!CryptographicOperations.FixedTimeEquals(
                Encoding.ASCII.GetBytes(expected),
                Encoding.ASCII.GetBytes(signature)))
        {
            error = "invalid signature";
            return false;
        }

        return true;
    }

    private static string ComputeHexHmacSha256(string payload, string secret)
    {
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(payload));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
