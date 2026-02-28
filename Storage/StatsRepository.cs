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

        await EnsureSchemaMigrationsTableAsync(connection);
        await ApplyPendingMigrationsAsync(connection);
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

    public async Task IngestTelemetryBatchAsync(IReadOnlyList<PreparedTelemetrySample> samples, CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(ct);

        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
                              INSERT INTO telemetry_samples (
                                  server_id, sample_ts, minute_bucket, hour_bucket,
                                  tps, mspt, cpu_usage_percent, ram_used_mb, ram_total_mb,
                                  network_rx_kbps, network_tx_kbps, online_players,
                                  ping_p50_ms, ping_p95_ms, ping_p99_ms
                              )
                              VALUES (
                                  $serverId, $sampleTs, $minuteBucket, $hourBucket,
                                  $tps, $mspt, $cpuUsagePercent, $ramUsedMb, $ramTotalMb,
                                  $networkRxKbps, $networkTxKbps, $onlinePlayers,
                                  $pingP50Ms, $pingP95Ms, $pingP99Ms
                              );
                              """;
        command.Parameters.Add("$serverId", SqliteType.Text);
        command.Parameters.Add("$sampleTs", SqliteType.Integer);
        command.Parameters.Add("$minuteBucket", SqliteType.Integer);
        command.Parameters.Add("$hourBucket", SqliteType.Integer);
        command.Parameters.Add("$tps", SqliteType.Real);
        command.Parameters.Add("$mspt", SqliteType.Real);
        command.Parameters.Add("$cpuUsagePercent", SqliteType.Real);
        command.Parameters.Add("$ramUsedMb", SqliteType.Real);
        command.Parameters.Add("$ramTotalMb", SqliteType.Real);
        command.Parameters.Add("$networkRxKbps", SqliteType.Real);
        command.Parameters.Add("$networkTxKbps", SqliteType.Real);
        command.Parameters.Add("$onlinePlayers", SqliteType.Integer);
        command.Parameters.Add("$pingP50Ms", SqliteType.Real);
        command.Parameters.Add("$pingP95Ms", SqliteType.Real);
        command.Parameters.Add("$pingP99Ms", SqliteType.Real);

        foreach (var sample in samples)
        {
            command.Parameters["$serverId"].Value = sample.ServerId;
            command.Parameters["$sampleTs"].Value = sample.SampleTs;
            command.Parameters["$minuteBucket"].Value = sample.MinuteBucket;
            command.Parameters["$hourBucket"].Value = sample.HourBucket;
            command.Parameters["$tps"].Value = DbValueOrNull(sample.Tps);
            command.Parameters["$mspt"].Value = DbValueOrNull(sample.Mspt);
            command.Parameters["$cpuUsagePercent"].Value = DbValueOrNull(sample.CpuUsagePercent);
            command.Parameters["$ramUsedMb"].Value = DbValueOrNull(sample.RamUsedMb);
            command.Parameters["$ramTotalMb"].Value = DbValueOrNull(sample.RamTotalMb);
            command.Parameters["$networkRxKbps"].Value = DbValueOrNull(sample.NetworkRxKbps);
            command.Parameters["$networkTxKbps"].Value = DbValueOrNull(sample.NetworkTxKbps);
            command.Parameters["$onlinePlayers"].Value = DbValueOrNull(sample.OnlinePlayers);
            command.Parameters["$pingP50Ms"].Value = DbValueOrNull(sample.PingP50Ms);
            command.Parameters["$pingP95Ms"].Value = DbValueOrNull(sample.PingP95Ms);
            command.Parameters["$pingP99Ms"].Value = DbValueOrNull(sample.PingP99Ms);
            await command.ExecuteNonQueryAsync(ct);
        }

        await transaction.CommitAsync(ct);
    }

    public async Task<TelemetryOverviewAggregate> QueryTelemetryOverviewAsync(
        string serverId,
        long startTs,
        long endTs,
        CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        var command = connection.CreateCommand();
        command.CommandText = """
                              SELECT
                                  COALESCE(AVG(tps), 0),
                                  COALESCE(AVG(mspt), 0),
                                  COALESCE(AVG(cpu_usage_percent), 0),
                                  COALESCE(AVG(ram_used_mb), 0),
                                  COALESCE(AVG(network_rx_kbps), 0),
                                  COALESCE(AVG(network_tx_kbps), 0),
                                  COALESCE(AVG(online_players), 0)
                              FROM telemetry_samples
                              WHERE server_id = $serverId
                                AND sample_ts >= $startTs
                                AND sample_ts <= $endTs;
                              """;
        command.Parameters.AddWithValue("$serverId", serverId);
        command.Parameters.AddWithValue("$startTs", startTs);
        command.Parameters.AddWithValue("$endTs", endTs);

        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return new TelemetryOverviewAggregate(0, 0, 0, 0, 0, 0, 0);
        }

        return new TelemetryOverviewAggregate(
            reader.GetDouble(0),
            reader.GetDouble(1),
            reader.GetDouble(2),
            reader.GetDouble(3),
            reader.GetDouble(4),
            reader.GetDouble(5),
            reader.GetDouble(6));
    }

    public async Task<IReadOnlyList<TelemetryPointDto>> QueryTelemetrySeriesByMinuteAsync(
        string serverId,
        long startTs,
        long endTs,
        CancellationToken ct)
    {
        return await QueryTelemetrySeriesAsync(serverId, startTs, endTs, "minute_bucket", ct);
    }

    public async Task<IReadOnlyList<TelemetryPointDto>> QueryTelemetrySeriesByHourAsync(
        string serverId,
        long startTs,
        long endTs,
        CancellationToken ct)
    {
        return await QueryTelemetrySeriesAsync(serverId, startTs, endTs, "hour_bucket", ct);
    }

    public async Task<int> QueryActivePlayersCountAsync(
        long startDayBucket,
        long endDayBucket,
        IReadOnlyCollection<Guid> blockedUuids,
        CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        var command = connection.CreateCommand();
        command.CommandText = """
                              SELECT COUNT(DISTINCT uuid)
                              FROM player_activity_daily
                              WHERE day_bucket >= $startDayBucket
                                AND day_bucket <= $endDayBucket
                              """;
        command.Parameters.AddWithValue("$startDayBucket", startDayBucket);
        command.Parameters.AddWithValue("$endDayBucket", endDayBucket);
        command.CommandText += BuildBlockedExclusionClause(command, blockedUuids, "uuid");

        var result = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt32(result);
    }

    public async Task<int> QueryNewPlayersCountAsync(
        long startDayBucket,
        long endDayBucket,
        IReadOnlyCollection<Guid> blockedUuids,
        CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        var command = connection.CreateCommand();
        command.CommandText = """
                              SELECT COUNT(*)
                              FROM player_first_seen
                              WHERE first_seen_day_bucket >= $startDayBucket
                                AND first_seen_day_bucket <= $endDayBucket
                              """;
        command.Parameters.AddWithValue("$startDayBucket", startDayBucket);
        command.Parameters.AddWithValue("$endDayBucket", endDayBucket);
        command.CommandText += BuildBlockedExclusionClause(command, blockedUuids, "uuid");

        var result = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt32(result);
    }

    public async Task<IReadOnlyDictionary<long, int>> QueryDailyActivePlayersAsync(
        long startDayBucket,
        long endDayBucket,
        IReadOnlyCollection<Guid> blockedUuids,
        CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        var command = connection.CreateCommand();
        command.CommandText = """
                              SELECT day_bucket, COUNT(*) AS player_count
                              FROM player_activity_daily
                              WHERE day_bucket >= $startDayBucket
                                AND day_bucket <= $endDayBucket
                              """;
        command.Parameters.AddWithValue("$startDayBucket", startDayBucket);
        command.Parameters.AddWithValue("$endDayBucket", endDayBucket);
        command.CommandText += BuildBlockedExclusionClause(command, blockedUuids, "uuid");
        command.CommandText += """

                               GROUP BY day_bucket;
                               """;

        var result = new Dictionary<long, int>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetInt64(0)] = reader.GetInt32(1);
        }

        return result;
    }

    public async Task<IReadOnlyDictionary<long, int>> QueryDailyNewPlayersAsync(
        long startDayBucket,
        long endDayBucket,
        IReadOnlyCollection<Guid> blockedUuids,
        CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        var command = connection.CreateCommand();
        command.CommandText = """
                              SELECT first_seen_day_bucket, COUNT(*) AS player_count
                              FROM player_first_seen
                              WHERE first_seen_day_bucket >= $startDayBucket
                                AND first_seen_day_bucket <= $endDayBucket
                              """;
        command.Parameters.AddWithValue("$startDayBucket", startDayBucket);
        command.Parameters.AddWithValue("$endDayBucket", endDayBucket);
        command.CommandText += BuildBlockedExclusionClause(command, blockedUuids, "uuid");
        command.CommandText += """

                               GROUP BY first_seen_day_bucket;
                               """;

        var result = new Dictionary<long, int>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetInt64(0)] = reader.GetInt32(1);
        }

        return result;
    }

    public async Task<IReadOnlyDictionary<long, int>> QueryHourlyActivePlayersAsync(
        long startHourBucket,
        long endHourBucket,
        IReadOnlyCollection<Guid> blockedUuids,
        CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        var command = connection.CreateCommand();
        command.CommandText = """
                              SELECT hour_bucket, COUNT(*) AS player_count
                              FROM player_activity_hourly
                              WHERE hour_bucket >= $startHourBucket
                                AND hour_bucket <= $endHourBucket
                              """;
        command.Parameters.AddWithValue("$startHourBucket", startHourBucket);
        command.Parameters.AddWithValue("$endHourBucket", endHourBucket);
        command.CommandText += BuildBlockedExclusionClause(command, blockedUuids, "uuid");
        command.CommandText += """

                               GROUP BY hour_bucket;
                               """;

        var result = new Dictionary<long, int>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetInt64(0)] = reader.GetInt32(1);
        }

        return result;
    }

    public async Task<bool> TryRegisterIngestIdempotencyKeyAsync(
        string endpointName,
        string serverId,
        string idempotencyKey,
        long createdAt,
        long expiresAt,
        CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);

        var cleanup = connection.CreateCommand();
        cleanup.CommandText = """
                              DELETE FROM ingest_idempotency
                              WHERE expires_at <= $now;
                              """;
        cleanup.Parameters.AddWithValue("$now", createdAt);
        await cleanup.ExecuteNonQueryAsync(ct);

        var insert = connection.CreateCommand();
        insert.CommandText = """
                             INSERT INTO ingest_idempotency (endpoint_name, server_id, idempotency_key, created_at, expires_at)
                             VALUES ($endpointName, $serverId, $idempotencyKey, $createdAt, $expiresAt)
                             ON CONFLICT(endpoint_name, server_id, idempotency_key) DO NOTHING;
                             """;
        insert.Parameters.AddWithValue("$endpointName", endpointName);
        insert.Parameters.AddWithValue("$serverId", serverId);
        insert.Parameters.AddWithValue("$idempotencyKey", idempotencyKey);
        insert.Parameters.AddWithValue("$createdAt", createdAt);
        insert.Parameters.AddWithValue("$expiresAt", expiresAt);
        var affected = await insert.ExecuteNonQueryAsync(ct);
        return affected > 0;
    }

    public async Task<IReadOnlyList<string>> QueryTelemetryServerIdsAsync(CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        var command = connection.CreateCommand();
        command.CommandText = """
                              SELECT DISTINCT server_id
                              FROM telemetry_samples
                              ORDER BY server_id ASC;
                              """;

        var result = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(reader.GetString(0));
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

    private static string BuildBlockedExclusionClause(
        SqliteCommand command,
        IReadOnlyCollection<Guid> blockedUuids,
        string uuidColumnName)
    {
        if (blockedUuids.Count == 0)
        {
            return string.Empty;
        }

        var parameters = new List<string>(blockedUuids.Count);
        var i = 0;
        foreach (var blocked in blockedUuids)
        {
            var name = $"$blocked{i++}";
            command.Parameters.AddWithValue(name, blocked.ToString());
            parameters.Add(name);
        }

        return $" AND {uuidColumnName} NOT IN ({string.Join(", ", parameters)})";
    }

    private static async Task EnsureSchemaMigrationsTableAsync(SqliteConnection connection)
    {
        var command = connection.CreateCommand();
        command.CommandText = """
                              CREATE TABLE IF NOT EXISTS schema_migrations (
                                  version INTEGER PRIMARY KEY,
                                  name TEXT NOT NULL,
                                  applied_at INTEGER NOT NULL
                              );
                              """;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task ApplyPendingMigrationsAsync(SqliteConnection connection)
    {
        var migrations = GetMigrations();
        foreach (var migration in migrations)
        {
            var existsCommand = connection.CreateCommand();
            existsCommand.CommandText = """
                                        SELECT COUNT(*)
                                        FROM schema_migrations
                                        WHERE version = $version;
                                        """;
            existsCommand.Parameters.AddWithValue("$version", migration.Version);
            var exists = Convert.ToInt32(await existsCommand.ExecuteScalarAsync()) > 0;
            if (exists)
            {
                continue;
            }

            await using var tx = (SqliteTransaction)await connection.BeginTransactionAsync();

            var migrateCommand = connection.CreateCommand();
            migrateCommand.Transaction = tx;
            migrateCommand.CommandText = migration.Sql;
            await migrateCommand.ExecuteNonQueryAsync();

            var insertCommand = connection.CreateCommand();
            insertCommand.Transaction = tx;
            insertCommand.CommandText = """
                                        INSERT INTO schema_migrations (version, name, applied_at)
                                        VALUES ($version, $name, $appliedAt);
                                        """;
            insertCommand.Parameters.AddWithValue("$version", migration.Version);
            insertCommand.Parameters.AddWithValue("$name", migration.Name);
            insertCommand.Parameters.AddWithValue("$appliedAt", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            await insertCommand.ExecuteNonQueryAsync();

            await tx.CommitAsync();
        }
    }

    private static IReadOnlyList<SchemaMigration> GetMigrations()
    {
        return
        [
            new SchemaMigration(
                1,
                "initial_core",
                """
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
                """
            ),
            new SchemaMigration(
                2,
                "telemetry_and_idempotency",
                """
                CREATE TABLE IF NOT EXISTS telemetry_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_id TEXT NOT NULL,
                    sample_ts INTEGER NOT NULL,
                    minute_bucket INTEGER NOT NULL,
                    hour_bucket INTEGER NOT NULL,
                    tps REAL NULL,
                    mspt REAL NULL,
                    cpu_usage_percent REAL NULL,
                    ram_used_mb REAL NULL,
                    ram_total_mb REAL NULL,
                    network_rx_kbps REAL NULL,
                    network_tx_kbps REAL NULL,
                    online_players INTEGER NULL,
                    ping_p50_ms REAL NULL,
                    ping_p95_ms REAL NULL,
                    ping_p99_ms REAL NULL
                );

                CREATE INDEX IF NOT EXISTS idx_telemetry_server_sample_ts
                    ON telemetry_samples (server_id, sample_ts);

                CREATE INDEX IF NOT EXISTS idx_telemetry_server_minute
                    ON telemetry_samples (server_id, minute_bucket);

                CREATE INDEX IF NOT EXISTS idx_telemetry_server_hour
                    ON telemetry_samples (server_id, hour_bucket);

                CREATE TABLE IF NOT EXISTS ingest_idempotency (
                    endpoint_name TEXT NOT NULL,
                    server_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    PRIMARY KEY (endpoint_name, server_id, idempotency_key)
                );

                CREATE INDEX IF NOT EXISTS idx_ingest_idempotency_expires
                    ON ingest_idempotency (expires_at);
                """
            )
        ];
    }

    private async Task<IReadOnlyList<TelemetryPointDto>> QueryTelemetrySeriesAsync(
        string serverId,
        long startTs,
        long endTs,
        string bucketColumn,
        CancellationToken ct)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        var command = connection.CreateCommand();
        command.CommandText = $"""
                               SELECT
                                   {bucketColumn},
                                   COALESCE(AVG(tps), 0),
                                   COALESCE(AVG(mspt), 0),
                                   COALESCE(AVG(cpu_usage_percent), 0),
                                   COALESCE(AVG(ram_used_mb), 0),
                                   COALESCE(AVG(network_rx_kbps), 0),
                                   COALESCE(AVG(network_tx_kbps), 0),
                                   COALESCE(AVG(online_players), 0)
                               FROM telemetry_samples
                               WHERE server_id = $serverId
                                 AND sample_ts >= $startTs
                                 AND sample_ts <= $endTs
                               GROUP BY {bucketColumn}
                               ORDER BY {bucketColumn} ASC;
                               """;
        command.Parameters.AddWithValue("$serverId", serverId);
        command.Parameters.AddWithValue("$startTs", startTs);
        command.Parameters.AddWithValue("$endTs", endTs);

        var points = new List<TelemetryPointDto>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            points.Add(new TelemetryPointDto(
                reader.GetInt64(0),
                reader.GetDouble(1),
                reader.GetDouble(2),
                reader.GetDouble(3),
                reader.GetDouble(4),
                reader.GetDouble(5),
                reader.GetDouble(6),
                reader.GetDouble(7)));
        }

        return points;
    }

    private static object DbValueOrNull(object? value)
    {
        return value ?? DBNull.Value;
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

public sealed record TelemetryOverviewAggregate(
    double AvgTps,
    double AvgMspt,
    double AvgCpuUsagePercent,
    double AvgRamUsedMb,
    double AvgNetworkRxKbps,
    double AvgNetworkTxKbps,
    double AvgOnlinePlayers
);

public sealed record SchemaMigration(int Version, string Name, string Sql);
