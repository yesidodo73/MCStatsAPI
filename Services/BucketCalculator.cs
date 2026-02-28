namespace McStats.Api.Services;

public sealed class BucketCalculator
{
    private readonly TimeZoneInfo _timeZone;

    public BucketCalculator(TimeZoneInfo timeZone)
    {
        _timeZone = timeZone;
    }

    public long ToMinuteBucketUnix(long unixTimeSeconds)
    {
        return unixTimeSeconds - (unixTimeSeconds % 60);
    }

    public long ToHourBucketUnix(long unixTimeSeconds)
    {
        return unixTimeSeconds - (unixTimeSeconds % 3600);
    }

    public long ToDayBucketUnix(long unixTimeSeconds)
    {
        var utc = DateTimeOffset.FromUnixTimeSeconds(unixTimeSeconds);
        var local = TimeZoneInfo.ConvertTime(utc, _timeZone);
        var localDate = new DateTimeOffset(local.Year, local.Month, local.Day, 0, 0, 0, local.Offset);
        var utcDate = TimeZoneInfo.ConvertTime(localDate, TimeZoneInfo.Utc);
        return utcDate.ToUnixTimeSeconds();
    }

    public long CurrentDayStartUnix(long nowUnix)
    {
        return ToDayBucketUnix(nowUnix);
    }

    public DateTimeOffset ToLocalTime(long unixTimeSeconds)
    {
        var utc = DateTimeOffset.FromUnixTimeSeconds(unixTimeSeconds);
        return TimeZoneInfo.ConvertTime(utc, _timeZone);
    }

    public string ToLocalDateString(long unixTimeSeconds)
    {
        var local = ToLocalTime(unixTimeSeconds);
        return $"{local.Year:D4}-{local.Month:D2}-{local.Day:D2}";
    }
}
