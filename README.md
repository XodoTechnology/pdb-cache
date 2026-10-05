# PeeringDB Cache

A drop-in caching API for PeeringDB.

Various network automation tools rely on PeeringDB for network information. `peeringdb-cache` creates a local mirror of PeeringDB and exposes a drop-in replacement query API, decreasing latency to PeeringDB resources and avoiding PeeringDB rate limits.

> Fork of [natesales/peeringdb-cache](https://github.com/natesales/peeringdb-cache) (no longer maintained). Fixes for upstream issues: syncs after the first silently doing nothing, `database is locked` errors during sync, missing `netixlan` `name`/`ix_id` fields, `threads can only be started once` on repeated `/sync` calls, and no restart/healthcheck support.

## Features

The cache supports the `org`, `fac`, `net`, `ix`, `campus`, `carrier`, `carrierfac`, `ixfac`, `ixlan`, `ixpfx`, `netfac`, `netixlan`, and `poc` resource types.

## Setup

See below to get started with Docker or [docker-compose](docker-compose.yml). The first sync downloads bulk snapshots from `public.peeringdb.com`, so initial sync is much faster than pulling every object through the API. You can also preload it with a `peeringdb.sqlite3` file in the data directory from another system that already has the database. Subsequent syncs only update the database with changes.

To get network contact information, set the `PEERINGDB_API_KEY` variable to your [PeeringDB API key](https://docs.peeringdb.com/howto/api_keys/).

Once your cache is running, you can point your tools to the cache instead of PeeringDB. It should be as easy as replacing the URL from `https://www.peeringdb.com/api` to `http://your-peeringdb-cache:8080/api`.

## Running a Sync

The cache will automatically sync with PeeringDB every 6 hours (configurable via `SYNC_INTERVAL`, in seconds). Syncs are incremental — they fetch only objects changed since the last sync.

Between scheduled syncs, the cache probes PeeringDB every 10 minutes (`CHANGE_CHECK_INTERVAL`, `0` disables) with a tiny `?since=` query per resource. If anything changed, a sync starts immediately — so the cache stays fresh without hammering the API. Set `CHANGE_CHECK_RESOURCES` to a comma-separated list of resource tags (e.g. `netixlan`) to limit probing to a single resource; changes to other resources are then only picked up at the regular `SYNC_INTERVAL`.

Failed syncs retry with exponential backoff. You can also manually trigger a sync by making a `GET` or `POST` request to `/sync`.

## Health Checks

- `GET /healthz` — liveness; 200 when the process and sync scheduler are alive, 503 otherwise. Wired into the Docker `HEALTHCHECK`.
- `GET /readyz` — readiness; 200 once the database is populated. Reports `last_sync`, `sync_running`, and `last_error`.

The compose file sets `restart: unless-stopped` so Docker restarts the container on a crash. Combine with a tool like autoheal if you also want restarts on failed healthchecks.

## Monitoring

A Prometheus metrics endpoint is available at `/metrics`. In addition to per-resource entry counts it exports `peeringdb_last_sync`, `peeringdb_next_sync`, `peeringdb_sync_running`, `peeringdb_sync_total`, `peeringdb_sync_duration_seconds`, `peeringdb_sync_consecutive_failures`, and `peeringdb_scheduler_alive`.

Alerting on `time() - peeringdb_last_sync > <threshold>` is a good way to catch a wedged sync.

## Configuration

Environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `PEERINGDB_API_KEY` | | PeeringDB API key; enables fetching private `poc`/`ixlan` data |
| `DATABASE_PATH` | `/data/peeringdb.sqlite3` | SQLite database location |
| `DATABASE_URL` | | Full database URL, e.g. `postgresql://user:pass@host:5432/peeringdb`; overrides the variables below |
| `DATABASE_ENGINE` | `sqlite3` | `sqlite3`, `postgresql` or `cockroachdb` |
| `DATABASE_NAME` | `peeringdb` (non-SQLite) | Database name |
| `DATABASE_HOST` / `DATABASE_PORT` | | Database host and port |
| `DATABASE_USER` / `DATABASE_PASSWORD` | | Database credentials |
| `DATABASE_SSLMODE` | | libpq `sslmode` (e.g. `require`); with `DATABASE_URL` use `?sslmode=...` |
| `SYNC_INTERVAL` | `21600` | Seconds between syncs |
| `CHANGE_CHECK_INTERVAL` | `600` | Seconds between remote-change probes; `0` disables |
| `CHANGE_CHECK_RESOURCES` | all synced | Comma-separated resource tags to probe |
| `PDB_SYNC_ONLY` | all | Comma-separated resource tags to sync (e.g. `org,net`) |
| `PDB_SYNC_URL` | `https://www.peeringdb.com/api` | PeeringDB API URL |
| `PDB_SYNC_CACHE_URL` | `https://public.peeringdb.com` | Bulk snapshot URL used for full syncs |
| `PEERINGDB_SYNC_TIMEOUT` | `60` | HTTP timeout (seconds) for PeeringDB requests |
| `PROXY_FIX_COUNT` | `0` | Number of trusted reverse proxies; enables `X-Forwarded-*` handling |
| `LOG_LEVEL` | `INFO` | Python logging level |

## PostgreSQL

SQLite is the default. To use PostgreSQL instead, create an empty database and set `DATABASE_URL` (or `DATABASE_ENGINE=postgresql` plus the `DATABASE_*` variables). Tables are created by migrations on startup. Preloading with a `.sqlite3` file only applies to SQLite.

PostgreSQL 14 or later is required (Django 5.2). The server's reported version is what counts, so CockroachDB, which reports 13 for wire compatibility, must use the dedicated backend instead: set `DATABASE_URL=cockroachdb://user@host:26257/peeringdb` (or `DATABASE_ENGINE=cockroachdb`) and create the database first.

## Docker

```bash
docker compose up -d --build
```

Or standalone:

```bash
docker build -t peeringdb-cache .
docker run \
    -d \
    --name peeringdb-cache \
    --restart unless-stopped \
    -v ./peeringdb-cache:/data \
    -p 8080:8080 \
    -e PEERINGDB_API_KEY=your-peeringdb-api-key \
    peeringdb-cache
```
