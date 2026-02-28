using McStats.Api.Configuration;
using McStats.Api.Services;
using System;
using System.Security.Cryptography;
using System.Text;
using Xunit;

namespace McStats.Api.Tests;

public sealed class IngestRequestAuthenticatorTests
{
    [Fact]
    public void Validate_ReturnsTrue_ForValidSignature()
    {
        var options = BuildOptions();
        var auth = new IngestRequestAuthenticator(new TestOptionsMonitor<McStatsOptions>(options));

        var ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        var body = "{\"events\":[{\"uuid\":\"11111111-1111-1111-1111-111111111111\",\"metric\":\"joins\",\"delta\":1}]}";
        var signature = ComputeSig(ts, body, "top-secret");

        var ok = auth.TryValidate("paper-main-1", "api-key-1", ts, signature, body, out var error);

        Assert.True(ok);
        Assert.Equal(string.Empty, error);
    }

    [Fact]
    public void Validate_ReturnsFalse_ForInvalidSignature()
    {
        var options = BuildOptions();
        var auth = new IngestRequestAuthenticator(new TestOptionsMonitor<McStatsOptions>(options));

        var ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        var body = "{\"samples\":[]}";

        var ok = auth.TryValidate("paper-main-1", "api-key-1", ts, "invalid", body, out var error);

        Assert.False(ok);
        Assert.Equal("invalid signature", error);
    }

    private static McStatsOptions BuildOptions()
    {
        return new McStatsOptions
        {
            DatabasePath = "data/test.db",
            TimeZoneId = "UTC",
            MaxClockSkewSeconds = 300,
            Security = new IngestSecurityOptions
            {
                Clients =
                [
                    new IngestClientCredential
                    {
                        ServerId = "paper-main-1",
                        ApiKey = "api-key-1",
                        Secret = "top-secret"
                    }
                ]
            }
        };
    }

    private static string ComputeSig(string ts, string body, string secret)
    {
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes($"{ts}\n{body}"));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
