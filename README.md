# Optikk Backend

Optikk Backend is the Go service layer for the Optikk observability platform. It accepts OTLP data over gRPC, persists telemetry into ClickHouse, serves product APIs over Gin, and uses MySQL and Redis for application state.

## Current architecture

The backend is a modular monolith with two main responsibilities:

- ingest OTLP spans, logs, and metrics over gRPC on port `4317`
- serve product and account APIs over HTTP on port `19090`

### Runtime pieces

- `cmd/server/main.go`: process entrypoint
- `internal/app/server/`: app bootstrap, HTTP router, gRPC server, health checks, infra wiring
- `internal/ingestion/`: OTLP handlers plus Kafka-backed ingest producers and consumers
- `internal/modules/`: product-facing HTTP and gRPC modules
- `internal/infra/`: shared database, Redis, Kafka, middleware, rollup, and session plumbing
- `internal/config/`: `config.yml` loading with `OPTIKK_*` environment overrides

### Storage and queues

- ClickHouse: analytical store for traces, logs, metrics, and rollups
- MySQL/MariaDB: users, teams, sessions metadata, and other relational application data
- Redis: required for sessions and response caching
- Kafka/Redpanda: required OTLP ingest queue between handlers and persistence consumers

## Module map

The current module manifest is assembled in [internal/app/server/modules_manifest.go](/Users/ramantayal/Desktop/pro/optikk-backend/internal/app/server/modules_manifest.go). Active areas include:

- overview: overview, RED metrics, HTTP metrics, SLO, error views, APM
- traces: query, explorer, trace detail
- logs: search and explorer
- metrics
- services: topology and deployments
- infrastructure: cpu, memory, disk, network, connpool, jvm, kubernetes, fleet, nodes, resource util
- saturation: kafka plus database collection, connections, errors, explorer, latency, slow queries, summary, system, systems, volume
- user: auth, team, user
- ingestion modules for spans, logs, and metrics

## HTTP and gRPC surfaces

- HTTP health: `GET /health`, `GET /health/live`, `GET /health/ready`
- Prometheus metrics: `GET /metrics`
- Product API base: `/api/v1`
- OTLP gRPC ingest: `localhost:4317`

`/api/v1` routes are registered per module. Cached routes use the Redis-backed cache middleware; uncached routes use the standard tenant-aware group.

## Local development

### Prerequisites

- Go
- Docker / Docker Compose

### Start local dependencies

From [docker-compose.yml](/Users/ramantayal/Desktop/pro/optikk-backend/docker-compose.yml):

- ClickHouse
- MariaDB
- Redis
- Redpanda

Run:

```bash
docker compose up -d
```

### Configure

The repo already contains a checked-in [config.yml](/Users/ramantayal/Desktop/pro/optikk-backend/config.yml) for local development. You can override values with `OPTIKK_*` environment variables.

Important local defaults:

- HTTP port: `19090`
- OTLP gRPC port: `4317`
- ClickHouse: `127.0.0.1:9000`
- MariaDB: `127.0.0.1:3306`
- Redis: `127.0.0.1:6379`
- Kafka broker: `localhost:19092`

### Run the backend

```bash
go run ./cmd/server
```

Or use the Make targets:

```bash
make run
make build
make vet
```

## Repo structure

```text
optikk-backend/
├── cmd/
│   ├── server/          # Main backend process
│   └── migrate/         # ClickHouse migration utility
├── db/
│   └── clickhouse/      # Embedded ClickHouse schema and migrations
├── docs/                # Focused design and ops docs
├── internal/
│   ├── app/             # App bootstrap and module registry
│   ├── config/          # Config structs and validation
│   ├── infra/           # Shared infrastructure building blocks
│   ├── ingestion/       # OTLP ingest pipeline
│   └── modules/         # Product modules
└── README.md
```

## Related docs

- Codebase map: [CODEBASE_INDEX.md](/Users/ramantayal/Desktop/pro/optikk-backend/CODEBASE_INDEX.md)
- Runtime metrics / Prometheus: [docs/observability.md](/Users/ramantayal/Desktop/pro/optikk-backend/docs/observability.md)
- ClickHouse notes: [db/clickhouse/README.md](/Users/ramantayal/Desktop/pro/optikk-backend/db/clickhouse/README.md)
