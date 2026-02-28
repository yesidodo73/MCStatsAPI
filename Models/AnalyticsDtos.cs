namespace McStats.Api.Models;

public sealed record TrendMetricDto(
    double Current,
    double Previous,
    double Delta,
    double ChangePercent,
    string Direction
);

public sealed record HourlyAveragePlayersDto(
    int Hour,
    double AveragePlayers
);

public sealed record DailyAnalyticsPointDto(
    string Date,
    int ActivePlayers,
    int NewPlayers
);

public sealed record AnalyticsOverviewDto(
    int Days,
    string WindowStartDate,
    string WindowEndDate,
    int NewPlayers,
    int ActivePlayers,
    double AverageDailyActivePlayers,
    IReadOnlyList<HourlyAveragePlayersDto> HourlyAveragePlayers,
    TrendMetricDto NewPlayersTrend,
    TrendMetricDto ActivePlayersTrend,
    TrendMetricDto AverageDailyActivePlayersTrend
);

public sealed record DailyAnalyticsResponse(
    int Days,
    string WindowStartDate,
    string WindowEndDate,
    IReadOnlyList<DailyAnalyticsPointDto> Points
);
