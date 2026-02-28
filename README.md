# mcstats

Initial backend implementation for querying Minecraft player statistics via API.

## Core Design

- Immutable raw data: `raw_events` allows only `INSERT`. `UPDATE`/`DELETE` are blocked by DB triggers.
- Pre-aggregated period storage:
  - `agg_minutely`: `last1h`
  - `agg_hourly`: `today`
  - `agg_daily`: `last7d`, `last30d`
  - `agg_total`: `total`
- Storage optimization:
  - Metrics are normalized through `metric_catalog` (string to integer ID) to reduce storage usage.
- UUID blocking:
  - Requests are blocked with HTTP 403 when a UUID exists in `mcstats.config.json` -> `BlockedUuids`.

## Configuration

All runtime settings are managed in `mcstats.config.json`.

```json
{
  "McStats": {
    "DatabasePath": "data/mcstats.db",
    "TimeZoneId": "Asia/Seoul",
    "BlockedUuids": [
      "00000000-0000-0000-0000-000000000000"
    ]
  }
}
```

## API

- `GET /healthz`
- `POST /v1/events/batch`
- `GET /v1/stats/{uuid}/{metric}?period=last1h|today|last7d|last30d|total`
- `GET /v1/stats/{uuid}?period=last1h|today|last7d|last30d|total`
- `GET /v1/config/blocked-uuids`
- `GET /v1/analytics/overview?days=7|30|90...`
- `GET /v1/analytics/daily?days=7|30|90...`
- `POST /v1/telemetry/batch`
- `GET /v1/telemetry/overview?serverId=paper-main-1&hours=24`
- `GET /v1/telemetry/series?serverId=paper-main-1&hours=24&resolution=hour|minute`
- `GET /v1/telemetry/servers`

Telemetry includes:
- TPS, MSPT
- CPU, RAM
- network Rx/Tx
- disk read/write (collector-dependent)
- GC collections per minute
- thread count
- online players and ping percentiles

## Ingest Security

Ingest endpoints require all headers below:

- `X-MCStats-ServerId`
- `X-MCStats-ApiKey`
- `X-MCStats-Timestamp`
- `X-MCStats-Signature` (HMAC-SHA256 hex of `timestamp + "\n" + rawBody`)
- `X-Idempotency-Key`

Configure allowed clients in `mcstats.config.json` under `McStats.Security.Clients`.

## Dashboard

- Open `/` to access the built-in analytics dashboard.
- Includes:
  - New players
  - Active players
  - Average daily active players
  - Hourly average players
  - Growth/decline trends against the previous same-length period

### Batch Ingest Request Example

```json
{
  "events": [
    {
      "uuid": "11111111-1111-1111-1111-111111111111",
      "metric": "play_time_seconds",
      "delta": 120,
      "timestampUtc": 1767235200
    }
  ]
}
```

## Run

```bash
dotnet restore
dotnet build
dotnet run
```
