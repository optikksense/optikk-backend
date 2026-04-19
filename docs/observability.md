# Observability — Prometheus & external forwarding

The backend exposes Prometheus-format runtime metrics (Go runtime, process
stats, HTTP handler stats) at `GET /metrics`. A Prometheus instance ships in
`docker-compose.yml` for local development; the same config forwards to any
Prometheus-remote-write-compatible vendor (Grafana Cloud, Datadog, Honeycomb,
Chronosphere, Mimir) by appending one `remote_write` block.

## Quick start (local)

```bash
docker compose up -d prometheus
# then run the API:
go run ./cmd/server
```

Prometheus UI: http://localhost:9090 → Status → Targets. You should see
`optikk-api`, `redpanda`, and `prometheus` as `UP`.

Sanity-check a query:
```
rate(promhttp_metric_handler_requests_total[1m])
```

## What gets scraped

`observability/prometheus.yml`:

| Job | Target | Notes |
|---|---|---|
| `optikk-api` | `host.docker.internal:8080/metrics` | Go runtime + HTTP + custom app metrics. |
| `redpanda` | `redpanda:9644/public_metrics` | Exposed natively by Redpanda. |
| `prometheus` | `localhost:9090/metrics` | Scrape loop sanity check. |

ClickHouse and MariaDB are not scraped by default — add a
`clickhouse-exporter` / `mysqld-exporter` sidecar if you need those.

## Forward to Grafana Cloud

1. In Grafana Cloud → **Connections → Add new connection → Hosted
   Prometheus metrics**, copy:
   - Remote-write endpoint (e.g. `https://prometheus-prod-XX.grafana.net/api/prom/push`)
   - Username (numeric instance ID)
   - API token (use a "MetricsPublisher" role)

2. Store them as env vars (don't commit):
   ```bash
   export GC_PROM_URL='https://prometheus-prod-XX.grafana.net/api/prom/push'
   export GC_PROM_USER='123456'
   export GC_PROM_TOKEN='glc_xxx...'
   ```

3. Append to `observability/prometheus.yml`:
   ```yaml
   remote_write:
     - url: ${GC_PROM_URL}
       basic_auth:
         username: ${GC_PROM_USER}
         password: ${GC_PROM_TOKEN}
       # Optional: drop high-cardinality series before shipping.
       # write_relabel_configs:
       #   - source_labels: [__name__]
       #     regex: "go_gc_pauses_seconds_bucket"
       #     action: drop
   ```

4. Enable env-var expansion in the Prometheus command line — replace the
   compose `command:` for the prometheus service with:
   ```yaml
   command:
     - --config.file=/etc/prometheus/prometheus.yml
     - --enable-feature=expand-external-labels
     - --web.enable-lifecycle
     - --storage.tsdb.path=/prometheus
     - --storage.tsdb.retention.time=2h   # shrink local TSDB if remote is source of truth
   ```
   And pass the env vars through:
   ```yaml
   environment:
     GC_PROM_URL: ${GC_PROM_URL}
     GC_PROM_USER: ${GC_PROM_USER}
     GC_PROM_TOKEN: ${GC_PROM_TOKEN}
   ```

5. Reload in place (no restart):
   ```bash
   curl -X POST http://localhost:9090/-/reload
   ```

   In Grafana Cloud → Explore, metrics should appear within ~30 s.

## Forward to Datadog

Datadog Agent accepts Prometheus remote_write on its OpenMetrics endpoint,
but the idiomatic path is **OpenTelemetry**: point the OTel Collector at
Datadog's OTLP intake. For metrics-only on a Prometheus stack:

```yaml
remote_write:
  - url: https://api.datadoghq.com/api/v1/series
    authorization:
      credentials: ${DD_API_KEY}
```

(Uses Datadog's Prometheus Remote Write API; see
https://docs.datadoghq.com/integrations/prometheus_remote_write/.)

## Forward to Honeycomb / Chronosphere / Mimir / self-hosted

Identical pattern — any Prometheus-remote-write endpoint:

```yaml
remote_write:
  - url: https://api.honeycomb.io/v1/metrics
    authorization:
      credentials: ${HONEYCOMB_API_KEY}
    headers:
      X-Honeycomb-Dataset: optikk
```

## Multiple destinations

`remote_write` is a list — add as many entries as you like. A common pattern
is ship to Grafana Cloud (long-term) and keep a local Prometheus (14d) for
debugging; both are already wired above.

## Drop high-cardinality series before shipping

`write_relabel_configs` runs per-sample before remote_write. Classic wins:

```yaml
write_relabel_configs:
  # Drop per-goroutine histograms (huge cardinality, rarely queried).
  - source_labels: [__name__]
    regex: "go_gc_pauses_seconds_bucket|go_sched_latencies_seconds_bucket"
    action: drop

  # Only ship span/trace metrics, drop Go runtime noise.
  # - source_labels: [__name__]
  #   regex: "optikk_.*|process_.*"
  #   action: keep
```

## Adding app-specific metrics

Use the default Prometheus client — register at init, increment/observe at
call sites:

```go
// internal/modules/foo/metrics.go
package foo

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var fooLatency = promauto.NewHistogramVec(
    prometheus.HistogramOpts{
        Name: "optikk_foo_duration_seconds",
        Help: "Latency of Foo operations.",
        Buckets: prometheus.DefBuckets,
    },
    []string{"op"},
)
```

Then in the handler:
```go
timer := prometheus.NewTimer(fooLatency.WithLabelValues("query"))
defer timer.ObserveDuration()
```

Metrics are automatically picked up by the `/metrics` endpoint on next scrape.

## Troubleshooting

- **`optikk-api` target `DOWN`**: the Go app isn't running on port 8080,
  OR you're on Linux without `host.docker.internal` (check the `extra_hosts`
  block in docker-compose; we set `host-gateway` which should resolve it).
- **`remote_write` failures**: Prometheus logs them at WARN — `docker compose
  logs prometheus | grep remote_write`.
- **Series stopped appearing remotely**: check the `prometheus_remote_storage_samples_dropped_total`
  metric on the local Prometheus.
