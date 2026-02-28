using McStats.Api.Configuration;
using McStats.Api.Models;
using McStats.Api.Services;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace McStats.Api.Storage;

public sealed class StatsRepository
{
    private readonly string _connectionString;
    private readonly Dictionary<string, long> _metricIdCache = new(StringComparer.Ordinal);
    private readonly object _metricIdCacheLock = new();

    public StatsRepository(IOptionsMonitor<McStatsOptions> options)
    {
        var dbPath = options.CurrentValue.DatabasePath;
        var directory = Path.GetDirectoryName(dbPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared,
            Pooling = true,
            DefaultTimeout = 5
        }.ToString();
    }

    public async Task InitializeAsync()
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();

        var pragma = connection.CreateCommand();
        pragma.CommandText = """
                             PRAGMA journal_mode = WAL;
                             PRAGMA synchronous = NORMAL;
                             PRAGMA foreign_keys = ON;
                             """;
        await pragma.ExecuteNonQueryAsync();

        var command = connection.CreateCommand();
        command.CommandText = """
                              CREATE TABLE IF NOT EXISTS metric_catalog (
                                  metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                  name TEXT NOT NULL UNIQUE
                              );

                              CREATE TABLE IF NOT EXISTS raw_events (
                                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                                  uuid TEXT NOT NULL,
                                  metric_id INTEGER NOT NULL,
                                  delta INTEGER NOT NULL,
                                  event_ts INTEGER NOT NULL,
                                  created_at INTEGER NOT NULL,
                                  FOREIGN KEY(metric_id) REFERENCES metric_catalog(metric_id)
                              );

                              CREATE INDEX IF NOT EXISTS idx_raw_events_uuid_metric_ts
                                  ON raw_events (uuid, metric_id, event_ts);

                              CREATE TABLE IF NOT EXISTS agg_minutely (
                                  uuid TEXT NOT NULL,
                                  metric_id INTEGER NOT NULL,
                                  bucket_start INTEGER NOT NULL,
                                  value INTEGER NOT NULL,
                                  PRIMARY KEY (uuid, metric_id, bucket_start),
                                  FOREIGN KEY(metric_id) REFERENCES metric_catalog(metric_id)
                              );

                              CREATE TABLE IF NOT EXISTS agg_hourly (
                                  uuid TEXT NOT NULL,
                                  metric_id INTEGER NOT NULL,
                                  bucket_start INTEGER NOT NULL,
                                  value INTEGER NOT NULL,
                                  PRIMARY KEY (uuid, metric_id, bucket_start),
                                  FOREIGN KEY(metric_id) REFERENCES metric_catalog(metric_id)
                              );

                              CREATE TABLE IF NOT EXISTS agg_daily (
                                  uuid TEXT NOT NULL,
                                  metric_id INTEGER NOT NULL,
                                  bucket_start INTEGER NOT NULL,
                                  value INTEGER NOT NULL,
                                  PRIMARY KEY (uuid, metric_id, bucket_start),
                                  FOREIGN KEY(metric_id) REFERENCES metric_catalog(metric_id)
                              );

                              CREATE TABLE IF NOT EXISTS agg_total (
                                  uuid TEXT NOT NULL,
                                  metric_id INTEGER NOT NULL,
                                  value INTEGER NOT NULL,
                                  PRIMARY KEY (uuid, metric_id),
                                  FOREIGN KEY(metric_id) REFERENCES metric_catalog(metric_id)
                              );

                              CREATE TABLE IF NOT EXISTS player_first_seen (
                                  uuid TEXT PRIMARY KEY,
                                  first_seen_ts INTEGER NOT NULL,
                                  first_seen_day_bucket INTEGER NOT NULL
                              );

                              CREATE TABLE IF NOT EXISTS player_activity_daily (
                                  uuid TEXT NOT NULL,
                                  day_bucket INTEGER NOT NULL,
                                  PRIMARY KEY (uuid, day_bucket)
                              );

                              CREATE INDEX IF NOT EXISTS idx_player_activity_daily_day
                                  ON player_activity_daily (day_bucket);

                              CREATE TABLE IF NOT EXISTS player_activity_hourly (
                                  uuid TEXT NOT NULL,
                                  hour_bucket INTEGER NOT NULL,
                                  PRIMARY KEY (uuid, hour_bucket)
                              );

                              CREATE INDEX IF NOT EXISTS idx_player_activity_hourly_hour
                                  ON player_activity_hourly (hour_bucket);

                              CREATE TRIGGER IF NOT EXISTS trg_no_update_raw_events
                              BEFORE UPDATE ON raw_events
                              BEGIN
                                  SELECT RAISE(ABORT, 'raw_events is immutable');
                              END;

                              CREATE TRIGGER IF NOT EXISTS trg_no_delete_raw_events
                              BEFORE DELETE ON raw_events
                              BEGIN
                                  SELECT RAISE(ABORT, 'raw_events is immutable');
                              END;
                              """;
        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> PingAsync(CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        var command = connection.CreateCommand();
        command.CommandText = "SELECT 1;";
        var result = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt32(result) == 1;
    }

    public async Task IngestBatchAsync(IReadOnlyList<PreparedEvent> events, CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(ct);

        var rawCommand = connection.CreateCommand();
        rawCommand.Transaction = transaction;
        rawCommand.CommandText = """
                                 INSERT INTO raw_events (uuid, metric_id, delta, event_ts, created_at)
                                 VALUES ($uuid, $metricId, $delta, $eventTs, $createdAt);
                                 """;
        rawCommand.Parameters.Add("$uuid", SqliteType.Text);
        rawCommand.Parameters.Add("$metricId", SqliteType.Integer);
        rawCommand.Parameters.Add("$delta", SqliteType.Integer);
        rawCommand.Parameters.Add("$eventTs", SqliteType.Integer);
        rawCommand.Parameters.Add("$createdAt", SqliteType.Integer);

        var minuteCommand = BuildAggregateUpsertCommand(connection, transaction, "agg_minutely");
        var hourCommand = BuildAggregateUpsertCommand(connection, transaction, "agg_hourly");
        var dayCommand = BuildAggregateUpsertCommand(connection, transaction, "agg_daily");
        var totalCommand = connection.CreateCommand();
        totalCommand.Transaction = transaction;
        totalCommand.CommandText = """
                                   INSERT INTO agg_total (uuid, metric_id, value)
                                   VALUES ($uuid, $metricId, $value)
                                   ON CONFLICT(uuid, metric_id) DO UPDATE SET value = value + excluded.value;
                                   """;
        totalCommand.Parameters.Add("$uuid", SqliteType.Text);
        totalCommand.Parameters.Add("$metricId", SqliteType.Integer);
        totalCommand.Parameters.Add("$value", SqliteType.Integer);

        var firstSeenCommand = connection.CreateCommand();
        firstSeenCommand.Transaction = transaction;
        firstSeenCommand.CommandText = """
                                       INSERT INTO player_first_seen (uuid, first_seen_ts, first_seen_day_bucket)
                                       VALUES ($uuid, $firstSeenTs, $firstSeenDayBucket)
                                       ON CONFLICT(uuid) DO UPDATE SET
                                           first_seen_ts = MIN(player_first_seen.first_seen_ts, excluded.first_seen_ts),
                                           first_seen_day_bucket = CASE
                                               WHEN excluded.first_seen_ts < player_first_seen.first_seen_ts
                                                   THEN excluded.first_seen_day_bucket
                                               ELSE player_first_seen.first_seen_day_bucket
                                           END;
                                       """;
        firstSeenCommand.Parameters.Add("$uuid", SqliteType.Text);
        firstSeenCommand.Parameters.Add("$firstSeenTs", SqliteType.Integer);
        firstSeenCommand.Parameters.Add("$firstSeenDayBucket", SqliteType.Integer);

        var dailyActivityCommand = connection.CreateCommand();
        dailyActivityCommand.Transaction = transaction;
        dailyActivityCommand.CommandText = """
                                           INSERT INTO player_activity_daily (uuid, day_bucket)
                                           VALUES ($uuid, $dayBucket)
                                           ON CONFLICT(uuid, day_bucket) DO NOTHING;
                                           """;
        dailyActivityCommand.Parameters.Add("$uuid", SqliteType.Text);
        dailyActivityCommand.Parameters.Add("$dayBucket", SqliteType.Integer);

        var hourlyActivityCommand = connection.CreateCommand();
        hourlyActivityCommand.Transaction = transaction;
        hourlyActivityCommand.CommandText = """
                                            INSERT INTO player_activity_hourly (uuid, hour_bucket)
                                            VALUES ($uuid, $hourBucket)
                                            ON CONFLICT(uuid, hour_bucket) DO NOTHING;
                                            """;
        hourlyActivityCommand.Parameters.Add("$uuid", SqliteType.Text);
        hourlyActivityCommand.Parameters.Add("$hourBucket", SqliteType.Integer);

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        foreach (var item in events)
        {
            var metricId = await ResolveMetricIdAsync(connection, transaction, item.Metric, ct);
            if (metricId is null)
            {
                throw new InvalidOperationException($"Failed to resolve metric id for '{item.Metric}'.");
            }

            rawCommand.Parameters["$uuid"].Value = item.Uuid.ToString();
            rawCommand.Parameters["$metricId"].Value = metricId.Value;
            rawCommand.Parameters["$delta"].Value = item.Delta;
            rawCommand.Parameters["$eventTs"].Value = item.EventTs;
            rawCommand.Parameters["$createdAt"].Value = now;
            await rawCommand.ExecuteNonQueryAsync(ct);

            await ExecuteAggregateCommand(minuteCommand, item.Uuid, metricId.Value, item.MinuteBucket, item.Delta, ct);
            await ExecuteAggregateCommand(hourCommand, item.Uuid, metricId.Value, item.HourBucket, item.Delta, ct);
            await ExecuteAggregateCommand(dayCommand, item.Uuid, metricId.Value, item.DayBucket, item.Delta, ct);

            totalCommand.Parameters["$uuid"].Value = item.Uuid.ToString();
            totalCommand.Parameters["$metricId"].Value = metricId.Value;
            totalCommand.Parameters["$value"].Value = item.Delta;
            await totalCommand.ExecuteNonQueryAsync(ct);

            firstSeenCommand.Parameters["$uuid"].Value = item.Uuid.ToString();
            firstSeenCommand.Parameters["$firstSeenTs"].Value = item.EventTs;
            firstSeenCommand.Parameters["$firstSeenDayBucket"].Value = item.DayBucket;
            await firstSeenCommand.ExecuteNonQueryAsync(ct);

            dailyActivityCommand.Parameters["$uuid"].Value = item.Uuid.ToString();
            dailyActivityCommand.Parameters["$dayBucket"].Value = item.DayBucket;
            await dailyActivityCommand.ExecuteNonQueryAsync(ct);

            hourlyActivityCommand.Parameters["$uuid"].Value = item.Uuid.ToString();
            hourlyActivityCommand.Parameters["$hourBucket"].Value = item.HourBucket;
            await hourlyActivityCommand.ExecuteNonQueryAsync(ct);
        }

        await transaction.CommitAsync(ct);
    }

    public async Task<long?> QueryMetricAsync(Guid uuid, string metric, Period period, long startBucket, CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        var command = connection.CreateCommand();

        if (period == Period.Total)
        {
            var metricId = await ResolveMetricIdAsync(connection, null, metric, ct, createIfMissing: false);
            if (metricId is null)
            {
                return null;
            }

            command.CommandText = """
                                  SELECT value
                                  FROM agg_total
                                  WHERE uuid = $uuid AND metric_id = $metricId;
                                  """;
            command.Parameters.AddWithValue("$uuid", uuid.ToString());
            command.Parameters.AddWithValue("$metricId", metricId.Value);
        }
        else
        {
            var (table, bucketColumn) = ResolveTable(period);
            var metricId = await ResolveMetricIdAsync(connection, null, metric, ct, createIfMissing: false);
            if (metricId is null)
            {
                return null;
            }

            command.CommandText = $"""
                                   SELECT COALESCE(SUM(value), 0)
                                   FROM {table}
                                   WHERE uuid = $uuid AND metric_id = $metricId AND {bucketColumn} >= $startBucket;
                                   """;
            command.Parameters.AddWithValue("$uuid", uuid.ToString());
            command.Parameters.AddWithValue("$metricId", metricId.Value);
            command.Parameters.AddWithValue("$startBucket", startBucket);
        }

        var value = await command.ExecuteScalarAsync(ct);
        if (value is null || value == DBNull.Value)
        {
            return null;
        }

        return Convert.ToInt64(value);
    }

    public async Task<IReadOnlyDictionary<string, long>> QueryAllMetricsAsync(Guid uuid, Period period, long startBucket, CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        var command = connection.CreateCommand();

        if (period == Period.Total)
        {
            command.CommandText = """
                                  SELECT c.name, t.value
                                  FROM agg_total t
                                  JOIN metric_catalog c ON c.metric_id = t.metric_id
                                  WHERE uuid = $uuid;
                                  """;
            command.Parameters.AddWithValue("$uuid", uuid.ToString());
        }
        else
        {
            var (table, bucketColumn) = ResolveTable(period);
            command.CommandText = $"""
                                   SELECT c.name, SUM(a.value) AS total_value
                                   FROM {table} a
                                   JOIN metric_catalog c ON c.metric_id = a.metric_id
                                   WHERE a.uuid = $uuid AND a.{bucketColumn} >= $startBucket
                                   GROUP BY c.name;
                                   """;
            command.Parameters.AddWithValue("$uuid", uuid.ToString());
            command.Parameters.AddWithValue("$startBucket", startBucket);
        }

        var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetString(0)] = reader.GetInt64(1);
        }

        return result;
    }

    private static SqliteCommand BuildAggregateUpsertCommand(SqliteConnection connection, SqliteTransaction transaction, string tableName)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
                               INSERT INTO {tableName} (uuid, metric_id, bucket_start, value)
                               VALUES ($uuid, $metricId, $bucketStart, $value)
                               ON CONFLICT(uuid, metric_id, bucket_start)
                               DO UPDATE SET value = value + excluded.value;
                               """;
        command.Parameters.Add("$uuid", SqliteType.Text);
        command.Parameters.Add("$metricId", SqliteType.Integer);
        command.Parameters.Add("$bucketStart", SqliteType.Integer);
        command.Parameters.Add("$value", SqliteType.Integer);
        return command;
    }

    private static async Task ExecuteAggregateCommand(SqliteCommand command, Guid uuid, long metricId, long bucket, long delta, CancellationToken ct)
    {
        command.Parameters["$uuid"].Value = uuid.ToString();
        command.Parameters["$metricId"].Value = metricId;
        command.Parameters["$bucketStart"].Value = bucket;
        command.Parameters["$value"].Value = delta;
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task<long?> ResolveMetricIdAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string metric,
        CancellationToken ct,
        bool createIfMissing = true)
    {
        lock (_metricIdCacheLock)
        {
            if (_metricIdCache.TryGetValue(metric, out var cachedId))
            {
                return cachedId;
            }
        }

        if (createIfMissing)
        {
            var insertCommand = connection.CreateCommand();
            insertCommand.Transaction = transaction;
            insertCommand.CommandText = """
                                        INSERT INTO metric_catalog (name)
                                        VALUES ($name)
                                        ON CONFLICT(name) DO NOTHING;
                                        """;
            insertCommand.Parameters.AddWithValue("$name", metric);
            await insertCommand.ExecuteNonQueryAsync(ct);
        }

        var selectCommand = connection.CreateCommand();
        selectCommand.Transaction = transaction;
        selectCommand.CommandText = """
                                    SELECT metric_id
                                    FROM metric_catalog
                                    WHERE name = $name;
                                    """;
        selectCommand.Parameters.AddWithValue("$name", metric);
        var metricIdObj = await selectCommand.ExecuteScalarAsync(ct);
        if (metricIdObj is null || metricIdObj == DBNull.Value)
        {
            return null;
        }

        var metricId = Convert.ToInt64(metricIdObj);
        lock (_metricIdCacheLock)
        {
            _metricIdCache[metric] = metricId;
        }

        return metricId;
    }

    private static (string Table, string BucketColumn) ResolveTable(Period period)
    {
        return period switch
        {
            Period.Last1H => ("agg_minutely", "bucket_start"),
            Period.Today => ("agg_hourly", "bucket_start"),
            Period.Last7D => ("agg_daily", "bucket_start"),
            Period.Last30D => ("agg_daily", "bucket_start"),
            _ => throw new InvalidOperationException($"Unsupported period: {period}")
        };
    }
}
