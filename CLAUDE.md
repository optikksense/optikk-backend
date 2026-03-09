Credentials

MySQL -
user: root
password: root123
database: observability

ClickHouse -
host: host.docker.internal
port: 9000
database: observability
username: default
password: clickhouse123

User Login:
username: user@example.com
password: securePassword123

## Project Structure

```
cmd/server/main.go              # Entry point

internal/
├── config/config.go            # Env-based configuration
├── contracts/                   # Shared DTOs, response helpers, context keys
├── database/
│   ├── clickhouse.go           # ClickHouse pool, wrapper, circuit breaker, PREWHERE/SETTINGS injection
│   ├── mysql.go                # MySQL pool, wrapper, timeout injection
│   ├── query.go                # QueryMaps, InClause, type coercion helpers
│   └── utils.go                # SqlTime, StringFromAny, Float64FromAny, etc.
├── helpers/                     # time.go (ParseRange), team.go (FromTeamUUID)
├── defaultconfig/               # Embedded YAML dashboard configs (pages/)
├── platform/
│   ├── server/app.go           # Gin setup, route wiring, graceful shutdown
│   ├── middleware/
│   │   ├── middleware.go       # Auth/tenant middleware, CORS
│   │   └── ratelimit.go       # In-process + Redis rate limiter
│   ├── ingest/
│   │   ├── ingest.go           # Batched ClickHouse insert queue (backpressure, Kafka)
│   │   └── tracker.go          # ByteTracker — per-team ingestion accounting
│   ├── otlp/
│   │   ├── handler.go          # OTLP/HTTP endpoints (/v1/traces, /v1/logs, /v1/metrics)
│   │   ├── mapper.go           # JSON OTLP → ingest.Row (spans, logs, metrics)
│   │   ├── model.go            # JSON OTLP request structs
│   │   ├── auth/auth.go        # API-key → team_id resolver
│   │   └── grpc/
│   │       ├── handler.go      # OTLP/gRPC service implementation
│   │       └── mapper.go       # Protobuf OTLP → ingest.Row
│   ├── auth/                    # JWT validation, token blacklist
│   ├── circuit_breaker/         # Generic circuit breaker (threshold=5, reset=30s)
│   ├── alerting/                # Alert rule engine
│   ├── timebucket/              # Adaptive time bucket expression for ClickHouse
│   └── utils/                   # String conversion helpers
└── modules/                     # Feature modules (each: models, repository, service, handler, module)
    ├── spans/
    │   ├── store.go             # Core span queries (aggregation, dependencies, trace list)
    │   ├── handler.go           # /v1/spans/* endpoints
    │   ├── tracedetail/         # /v1/traces/:traceId/* (span events, critical path, etc.)
    │   ├── redmetrics/          # RED metrics (top-slow, top-error, apdex, scorecard)
    │   └── errortracking/       # Error tracking (exception rate, error hotspot, 5xx by route)
    ├── services/
    │   ├── service/             # Service list, service detail
    │   ├── servicemap/          # Upstream/downstream deps, external dependencies, client-server latency
    │   └── topology/            # Service topology graph
    ├── overview/
    │   ├── overview/            # Overview dashboard aggregations
    │   ├── errors/              # Error overview
    │   └── slo/                 # SLO/SLI calculations
    ├── log/                     # Log search and aggregation
    ├── infrastructure/
    │   ├── nodes/               # Host/node metrics
    │   ├── resource_utilisation/ # CPU, memory, disk, network, JVM metrics
    │   └── kubernetes/          # K8s pod/container metrics
    ├── saturation/
    │   ├── database/            # DB saturation (MySQL, Postgres, Redis)
    │   └── kafka/               # Kafka consumer lag, throughput
    ├── apm/                     # APM dashboard
    ├── httpmetrics/             # HTTP metrics from OTel
    ├── ai/                      # AI-powered query builder
    ├── user/                    # Auth, OAuth, user/team management
    ├── defaultconfig/           # Dashboard config CRUD API
    └── common/                  # Shared handler base (DBTenant, GetTenantFunc)

db/                              # ClickHouse + MySQL schema DDL
docs/                            # Documentation
  └── resource_estimation/       # Production scaling plan, load testing guide
```
