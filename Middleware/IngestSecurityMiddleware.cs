using McStats.Api.Services;

namespace McStats.Api.Middleware;

public sealed class IngestSecurityMiddleware
{
    private readonly RequestDelegate _next;

    public IngestSecurityMiddleware(RequestDelegate next)
    {
        _next = next;
    }

    public async Task InvokeAsync(
        HttpContext context,
        IngestRequestAuthenticator authenticator,
        IngestIdempotencyService idempotencyService)
    {
        if (!HttpMethods.IsPost(context.Request.Method))
        {
            await _next(context);
            return;
        }

        var endpointName = ResolveEndpointName(context.Request.Path);
        if (endpointName is null)
        {
            await _next(context);
            return;
        }

        if (!TryReadHeader(context, "X-MCStats-ServerId", out var serverId) ||
            !TryReadHeader(context, "X-MCStats-ApiKey", out var apiKey) ||
            !TryReadHeader(context, "X-MCStats-Timestamp", out var timestamp) ||
            !TryReadHeader(context, "X-MCStats-Signature", out var signature) ||
            !TryReadHeader(context, "X-Idempotency-Key", out var idempotencyKey))
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            await context.Response.WriteAsJsonAsync(new { error = "missing required ingest auth headers" });
            return;
        }

        if (idempotencyKey.Length > 128)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new { error = "idempotency key too long" });
            return;
        }

        context.Request.EnableBuffering();
        using var reader = new StreamReader(context.Request.Body, leaveOpen: true);
        var rawBody = await reader.ReadToEndAsync(context.RequestAborted);
        context.Request.Body.Position = 0;

        if (!authenticator.TryValidate(serverId, apiKey, timestamp, signature, rawBody, out var error))
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            await context.Response.WriteAsJsonAsync(new { error });
            return;
        }

        var registered = await idempotencyService.TryRegisterAsync(endpointName, serverId, idempotencyKey, context.RequestAborted);
        if (!registered)
        {
            context.Items["ingest.replay"] = true;
        }

        context.Items["ingest.serverId"] = serverId;
        await _next(context);
    }

    private static bool TryReadHeader(HttpContext context, string name, out string value)
    {
        value = string.Empty;
        if (!context.Request.Headers.TryGetValue(name, out var headerValues))
        {
            return false;
        }

        var raw = headerValues.ToString().Trim();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        value = raw;
        return true;
    }

    private static string? ResolveEndpointName(PathString path)
    {
        if (path.Equals("/v1/events/batch", StringComparison.OrdinalIgnoreCase))
        {
            return "events";
        }

        if (path.Equals("/v1/telemetry/batch", StringComparison.OrdinalIgnoreCase))
        {
            return "telemetry";
        }

        return null;
    }
}
