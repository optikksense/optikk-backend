# Optikk Backend — Query-Side Load Test

k6 load test exercising every read endpoint exposed by `optikk-backend`.
Ingestion-side load generation lives in a separate tool — this directory
covers query traffic only.

## Prerequisites

- **k6** binary on `PATH`. Either:
  - `brew install k6` (macOS), or
  - run via Docker: `docker run --rm -i grafana/k6 run - <loadtest/entrypoints/smoke.js`
- **Backend running locally** at `:19090` (`make run` from `optikk-backend/`).
- **MySQL, ClickHouse, Redis** up — the bootstrap step talks to MySQL via
  `POST /api/v1/teams` + `POST /api/v1/users`, and queries hit ClickHouse.
- **(Optional) Prometheus stack** at `:19091` with the remote-write receiver
  enabled — see [Prometheus output](#prometheus-output) below.


# Load Test Working
docker run --rm \
  -v $(pwd)/loadtest:/loadtest \
  -e BASE_URL=http://host.docker.internal:19090 \
  -e RPS=20 -e VUS=5 -e DURATION=2m \
  grafana/k6 run /loadtest/entrypoints/all.js

## Quickstart

```bash
# Smoke (one endpoint per module, single iteration)
k6 run loadtest/entrypoints/smoke.js

# Single module at 20 RPS for 2 minutes
k6 run -e RPS=20 -e DURATION=2m loadtest/entrypoints/traces.js

# Full sweep, JSON + Prometheus output
k6 run \
  -e RPS=50 -e DURATION=5m -e VUS=10 \
  -e JSON_OUT=out/full.json \
  --out experimental-prometheus-rw=http://localhost:19091/api/v1/write \
  loadtest/entrypoints/all.js
```

## Bootstrap (creates a team + user in MySQL)

The first invocation against a fresh DB hits two public endpoints:

1. `POST /api/v1/teams` — creates `Loadtest Org` / `Loadtest Team`.
2. `POST /api/v1/users` — creates the user from `EMAIL` / `PASSWORD`.
3. `POST /api/v1/auth/login` — returns the `optikk_session` cookie.

Subsequent invocations skip steps 1–2 (login succeeds) and only step 3 runs.

A safety rail in [`lib/bootstrap.js`](../lib/bootstrap.js) refuses the
create flow against any host that does not match `localhost`, `127.0.0.1`,
`host.docker.internal`, or contain `dev`/`staging`/`loadtest`. Override
with `-e ALLOW_REMOTE_BOOTSTRAP=1`.

## Configuration flags

All flags are set with `-e KEY=VALUE`.

| Flag | Default | Purpose |
|---|---|---|
| `BASE_URL` | `http://localhost:19090` | Backend base URL (no trailing slash) |
| `EMAIL` | `loadtest@optikk.local` | Login email; auto-created on first run |
| `PASSWORD` | `optikk-loadtest` | Login password |
| `TEAM_NAME` | `Loadtest Team` | Team name to create on first run |
| `ORG_NAME` | `Loadtest Org` | Org name to create on first run |
| `TEAM_ID` | _unset_ | Optional `X-Team-Id` override |
| `RPS` | `10` | Per-scenario arrival rate (per second) |
| `DURATION` | `1m` | Run duration (k6 format: `30s`, `5m`, `1h`) |
| `VUS` | `50` | Pre-allocated VUs per scenario |
| `JSON_OUT` | _unset_ | Path for `handleSummary` JSON file |
| `LOOKBACK` | `1h` | Time-window for query payloads (`5m`/`15m`/`1h`/`6h`/`24h`/`7d`) |
| `BYPASS_CACHE` | `1` | Sends `Cache-Control: no-cache` + `X-Optikk-Bypass-Cache: 1` so dashboard SLA is measured without response-cache help |
| `ALLOW_REMOTE_BOOTSTRAP` | `0` | Required to bootstrap on non-local hosts |

## Output channels

All four are wired in:

1. **Stdout summary** — k6 default; `handleSummary` re-emits the built-in
   text summary so the colored end-of-run table is preserved.
2. **Live progress ticker** — k6 default; per-second VU + RPS bar appears
   while the test runs.
3. **JSON results file** — set `JSON_OUT=out/run.json`. The file contains
   the raw k6 metrics blob plus a per-`{module, endpoint}` rollup.
4. **Prometheus remote-write** — pass
   `--out experimental-prometheus-rw=http://localhost:19091/api/v1/write`.
   Custom Trends from [`lib/metrics.js`](../lib/metrics.js) include
   `loadtest_request_duration_ms`, `loadtest_request_errors_total`,
   `loadtest_request_success_rate` — all tagged with `{module, endpoint}`.

### Prometheus output

The local Prometheus stack at
[`monitoring/stack/docker-compose.yml`](../../monitoring/stack/docker-compose.yml)
must accept remote-writes. Today it only scrapes — add the receiver flag
to the `prometheus` service `command:` block:

```yaml
command:
  - "--config.file=/etc/prometheus/prometheus.yml"
  - "--storage.tsdb.path=/prometheus"
  - "--storage.tsdb.retention.time=7d"
  - "--web.enable-lifecycle"
  - "--web.enable-remote-write-receiver"   # add this
```

Then restart with
`docker compose -f monitoring/stack/docker-compose.yml up -d`.

## Layout

```
loadtest/
  scenarios/<module>/<file>.js   one scenario per file, exports named exec fns
  lib/                           shared helpers (config, auth, client, payloads, ...)
  entrypoints/                   k6 run targets — compose scenarios into options.scenarios
  docs/README.md                 this file
```

Module + endpoint coverage is enumerated by entrypoint:

- [`entrypoints/traces.js`](../entrypoints/traces.js) — explorer, analytics,
  spans query, suggest, detail (core + extras), paths/shape, errors, latency.
- [`entrypoints/logs.js`](../entrypoints/logs.js) — explorer, analytics, detail.
- [`entrypoints/metrics.js`](../entrypoints/metrics.js) — explorer query + meta.
- [`entrypoints/overview.js`](../entrypoints/overview.js) — errors,
  span errors, SLO, RED metrics, HTTP metrics, APM.
- [`entrypoints/infrastructure.js`](../entrypoints/infrastructure.js) — nodes,
  fleet, compute (cpu/mem/jvm), I/O (disk/net/connpool), resource utilization,
  kubernetes.
- [`entrypoints/saturation.js`](../entrypoints/saturation.js) — datastores
  explorer + drilldown (drilldown walks every database submodule), kafka
  explorer + perf + lag + health.
- [`entrypoints/services.js`](../entrypoints/services.js) — topology, deployments.
- [`entrypoints/all.js`](../entrypoints/all.js) — every scenario, weighted.
- [`entrypoints/smoke.js`](../entrypoints/smoke.js) — one endpoint per module
  in a single VU iteration.
- [`entrypoints/granular.js`](../entrypoints/granular.js) — every submodule as
  its own k6 scenario. Use this for per-submodule throughput/latency
  measurement: `db_volume`, `db_errors`, `db_latency`, `db_collection`,
  `db_system`, `db_systems`, `db_summary`, `db_slow_queries`,
  `db_connections`, `infra_cpu`, `infra_memory`, `infra_jvm`, `infra_disk`,
  `infra_network`, `infra_connpool`, `infra_nodes`, `infra_fleet`,
  `infra_kubernetes`. Run a single submodule with
  `--scenario=<name>`; total RPS scales with the number of active scenarios,
  so dial `RPS` down for whole-file runs.

### Submodule layout

Per-submodule scenario files live next to their composite parent:

```
loadtest/scenarios/
  saturation/
    datastores_explorer.js          # composer / explorer endpoints
    datastores_drilldown.js         # orchestrator over database/*.js
    database/
      volume.js          errors.js          latency.js
      collection.js      system.js          systems.js
      summary.js         slowqueries.js     connections.js
    kafka_*.js                      # explorer, perf, lag, health
  infrastructure/
    nodes.js     fleet.js     kubernetes.js
    compute.js   # orchestrator over cpu.js, memory.js, jvm.js
    io.js        # orchestrator over disk.js, network.js, connpool.js
    cpu.js       memory.js    jvm.js
    disk.js      network.js   connpool.js
```

Each submodule file exports one named function (`dbVolume`, `infraCPU`, …)
that does a full sweep of its routes once per call. Composites just call
the submodule functions in order, so existing `saturation.js` /
`infrastructure.js` / `all.js` entrypoints keep working unchanged.

## What to watch in Grafana

While the load test runs, the existing dashboards in
[`monitoring/grafana/dashboards/`](../../monitoring/grafana/dashboards/)
will tell you how the backend is responding:

- `optikk_http_api` — per-API drill-down (request rate, p95, error rate).
- `optikk_db` — ClickHouse/MySQL query latency, error rate, slow queries.
- `optikk_redis` — useful only if you deliberately re-enable cached runs
  with `-e BYPASS_CACHE=0`.

If you've enabled the Prometheus remote-write receiver, you'll also get
the `loadtest_*` series — query
`loadtest_request_duration_ms{module="traces"}` to see per-endpoint p95
in real time.

## Common failure modes

- **`bootstrap: refusing to create users on '...'`** — the safety rail
  fired. Either point `BASE_URL` at localhost, or set
  `ALLOW_REMOTE_BOOTSTRAP=1`.
- **`bootstrap: createUser failed status=400 body=Email is invalid`** —
  the `EMAIL` flag isn't a real email format.
- **All scenarios fail with `envelope success=true` check** — likely a
  401: the session cookie was rejected. Check that the user exists and
  has at least one team membership (the auth service rejects users with
  no teams — see [`internal/modules/user/auth/service.go:123`](../../internal/modules/user/auth/service.go#L123)).
- **k6 says `unknown executor: experimental-prometheus-rw`** — wrong
  flag spelling. The k6 output flag is
  `--out experimental-prometheus-rw=URL`, not `--executor=...`.
- **Unexpected cache hits during a no-cache run** — confirm the test is using
  the default `-e BYPASS_CACHE=1`, which sends both cache-bypass headers.
