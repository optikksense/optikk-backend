# Optikk Backend — Codebase Index

Orientation for [optikk-backend](/Users/ramantayal/Desktop/pro/optikk-backend). This file is meant to reflect the codebase as it exists now.

## Snapshot

- Stack: Go, Gin, gRPC, ClickHouse, MySQL/MariaDB, Redis, Kafka/Redpanda
- Entry point: [cmd/server/main.go](/Users/ramantayal/Desktop/pro/optikk-backend/cmd/server/main.go)
- App bootstrap: [internal/app/server/app.go](/Users/ramantayal/Desktop/pro/optikk-backend/internal/app/server/app.go)
- Module manifest: [internal/app/server/modules_manifest.go](/Users/ramantayal/Desktop/pro/optikk-backend/internal/app/server/modules_manifest.go)
- HTTP router: [internal/app/server/routes.go](/Users/ramantayal/Desktop/pro/optikk-backend/internal/app/server/routes.go)
- Config source: [config.yml](/Users/ramantayal/Desktop/pro/optikk-backend/config.yml)

## Architecture

### Bootstrap

- `cmd/server/main.go` loads config, sets up logging, creates the app, and starts the HTTP and gRPC servers.
- `internal/app/server/app.go` wires infra, loads configured modules, starts background runners, and manages shutdown.
- `internal/app/server/routes.go` builds the Gin router, health endpoints, `/metrics`, and `/api/v1` route groups.

### Ingestion

`internal/ingestion/` owns OTLP ingest for spans, logs, and metrics.

- `handler.go`: gRPC-facing OTLP handlers
- `mapper*.go`: transform OTLP payloads into internal row models
- `producer.go`: enqueue ingest records to Kafka
- `consumer.go`: background persistence consumers
- `row.go` and generated `*.pb.go`: internal row formats
- `module.go`: module registration

Current queueing model is Kafka-backed, not Redis-stream-backed. Local development uses Redpanda from [docker-compose.yml](/Users/ramantayal/Desktop/pro/optikk-backend/docker-compose.yml).

### Data and platform infrastructure

- `internal/infra/database/`: MySQL and ClickHouse clients, migration helpers
- `internal/infra/kafka/`: broker client, producers, consumers, topic helpers
- `internal/infra/redis/`: Redis client
- `internal/infra/session/`: session persistence and middleware integration
- `internal/infra/middleware/`: recovery, CORS, tenant, body limit, response cache
- `internal/infra/rollup/`: time-range-aware rollup tier selection
- `internal/infra/cursor/`: cursor helpers for explorer-style APIs

### Module shape

Most feature modules still follow the familiar package split:

- `module.go`
- `handler.go`
- `service.go`
- `repository.go`
- `dto.go` where needed
- `models.go`

Not every package uses the exact same six-file layout, so treat the manifest and package contents as the source of truth.

## Request surfaces

### HTTP

- Health: `/health`, `/health/live`, `/health/ready`
- Prometheus metrics: `/metrics`
- Product APIs: `/api/v1/...`

`/api/v1` has two route groups:

- uncached routes with tenant middleware
- cached routes with tenant middleware plus Redis-backed response caching

Modules choose the target group through the registry contract.

### gRPC

- OTLP gRPC server listens on `otlp.grpc_port`
- auth is enforced through gRPC interceptors in `internal/auth`

## Current module inventory

From the live module manifest:

- overview: `overview`, `redmetrics`, `httpmetrics`, `errors`, `slo`, `apm`
- traces: `query`, `explorer`, `tracedetail`
- logs: `search`, `explorer`
- metrics
- services: `topology`, `deployments`
- infrastructure: `connpool`, `cpu`, `disk`, `fleet`, `jvm`, `kubernetes`, `memory`, `network`, `nodes`, `resourceutil`
- saturation: `kafka`, `database/collection`, `connections`, `errors`, `explorer`, `latency`, `slowqueries`, `summary`, `system`, `systems`, `volume`
- user: `auth`, `team`, `user`
- ingestion: spans, logs, metrics

## Key directories

| Path | Purpose |
|------|---------|
| `cmd/server/` | Main server binary |
| `cmd/migrate/` | ClickHouse migration entrypoint |
| `db/clickhouse/` | ClickHouse schema and migration files |
| `internal/app/` | App composition and registry |
| `internal/auth/` | HTTP/gRPC auth helpers |
| `internal/config/` | Config structs, defaults, validation |
| `internal/infra/` | Cross-cutting infra packages |
| `internal/ingestion/` | OTLP ingest pipeline |
| `internal/modules/` | Product/domain APIs |
| `internal/shared/` | Shared contracts and helpers |
| `docs/` | Supporting design and ops docs |

## Local runbook

Start local dependencies:

```bash
docker compose up -d
```

Run the backend:

```bash
go run ./cmd/server
```

Useful commands:

```bash
make run
make build
make vet
go test ./...
```

## Related docs

- Overview doc: [README.md](/Users/ramantayal/Desktop/pro/optikk-backend/README.md)
- Observability doc: [docs/observability.md](/Users/ramantayal/Desktop/pro/optikk-backend/docs/observability.md)
- Frontend sibling repo: [../optikk-frontend/README.md](/Users/ramantayal/Desktop/pro/optikk-frontend/README.md)
