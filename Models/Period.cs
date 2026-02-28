namespace McStats.Api.Models;

public enum Period
{
    Last1H,
    Today,
    Last7D,
    Last30D,
    Total
}

public static class PeriodExtensions
{
    public static bool TryParse(string value, out Period period)
    {
        var normalized = value.ToLowerInvariant();
        period = normalized switch
        {
            "last1h" => Period.Last1H,
            "today" => Period.Today,
            "last7d" => Period.Last7D,
            "last30d" => Period.Last30D,
            "total" => Period.Total,
            _ => default
        };

        return normalized is "last1h" or "today" or "last7d" or "last30d" or "total";
    }

    public static string ToApiString(this Period period) =>
        period switch
        {
            Period.Last1H => "last1h",
            Period.Today => "today",
            Period.Last7D => "last7d",
            Period.Last30D => "last30d",
            Period.Total => "total",
            _ => throw new ArgumentOutOfRangeException(nameof(period), period, null)
        };
}
