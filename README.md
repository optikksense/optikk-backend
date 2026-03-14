# Optikk Lens Backend

The Go/Gin backend service for the Optikk observability platform. It provides a REST API for telemetry data (traces, logs, metrics), JWT authentication, multi-tenant support, and real-time ingestion via OpenTelemetry Protocol (OTLP). Data is persisted in MySQL (config, users) and ClickHouse (telemetry).

## Quick Start

### Prerequisites

- Docker and Docker Compose, or Podman
- MySQL 8.0+ (or MariaDB 11.0+)
- ClickHouse 24.0+
- An initialized database schema (see [Database Setup](#database-setup))

### Database Setup

Before running the backend, initialize the databases:

#### 1. Start MySQL

```bash
docker run -d \
  --name mysql \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root123 \
  -e MYSQL_DATABASE=observability \
  mariadb:11.4
```

#### 2. Start ClickHouse

```bash
docker run -d \
  --name clickhouse \
  -p 9000:9000 \
  -p 8123:8123 \
  --ulimit nofile=262144:262144 \
  -e CLICKHOUSE_DB=observability \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=clickhouse123 \
  clickhouse/clickhouse-server:26.2
```

### Running the Backend

#### Pull the Docker Image

```bash
docker pull ghcr.io/optikk-org/optikk-lens:latest
```

#### Run the Container

```bash
docker run -d \
  --name optikk-backend \
  -p 9090:9090 \
  -e GO_ENV=production \
  -e PORT=9090 \
  -e MYSQL_HOST=host.docker.internal \
  -e MYSQL_PORT=3306 \
  -e MYSQL_USERNAME=root \
  -e MYSQL_PASSWORD=root123 \
  -e MYSQL_DATABASE=observability \
  -e CLICKHOUSE_HOST=host.docker.internal \
  -e CLICKHOUSE_PORT=9000 \
  -e CLICKHOUSE_USERNAME=default \
  -e CLICKHOUSE_PASSWORD=clickhouse123 \
  -e CLICKHOUSE_DATABASE=observability \
  -e ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173 \
  -e JWT_SECRET=your_super_secret_jwt_key_change_this_in_production \
  -e REDIS_ENABLED=false \
  -e KAFKA_ENABLED=false \
  ghcr.io/optikk-org/optikk-lens:latest
```

**Note:** If databases are on your host machine (not in Docker), use `host.docker.internal` instead of `localhost`.

### Environment Variables

| Variable | Description | Default / Required |
|---|---|---|
| `GO_ENV` | Environment: `development` or `production` | `development` |
| `PORT` | HTTP server port | `9090` |
| `MYSQL_HOST` | MySQL hostname | Required |
| `MYSQL_PORT` | MySQL port | `3306` |
| `MYSQL_USERNAME` | MySQL username | Required |
| `MYSQL_PASSWORD` | MySQL password | Required |
| `MYSQL_DATABASE` | MySQL database name | `observability` |
| `CLICKHOUSE_HOST` | ClickHouse hostname | Required |
| `CLICKHOUSE_PORT` | ClickHouse native protocol port | `9000` |
| `CLICKHOUSE_USERNAME` | ClickHouse username | Required |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | Required |
| `CLICKHOUSE_DATABASE` | ClickHouse database name | `observability` |
| `ALLOWED_ORIGINS` | CORS allowed origins (comma-separated) | `http://localhost:3000,http://localhost:5173` |
| `JWT_SECRET` | Secret key for signing JWT tokens (min 32 chars) | Required in production |
| `REDIS_ENABLED` | Enable Redis for token blacklist & pub/sub | `false` |
| `REDIS_HOST` | Redis hostname (if enabled) | `localhost` |
| `REDIS_PORT` | Redis port (if enabled) | `6379` |
| `KAFKA_ENABLED` | Enable Kafka for batched ingest queuing | `false` |
| `KAFKA_BROKERS` | Kafka broker addresses (if enabled) | `localhost:9092` |

### Verify the Backend is Running

```bash
# Check container is running
docker ps | grep optikk-backend

# Check health
curl http://localhost:9090/api/v1/health

# View logs
docker logs optikk-backend
```

---

## Full Stack Deployment with Podman

For VM/production deployments, use Podman to run the entire stack on a single network.

```bash
# Create network for inter-container communication
podman network create observability-net

# 1. Start MySQL
podman run -d --name mysql --network observability-net \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root123 \
  -e MYSQL_DATABASE=observability \
  -v mysql-data:/var/lib/mysql \
  mariadb:11.4

# 2. Start ClickHouse
podman run -d --name clickhouse --network observability-net \
  -p 9000:9000 -p 8123:8123 \
  -e CLICKHOUSE_DB=observability \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=clickhouse123 \
  --ulimit nofile=262144:262144 \
  -v clickhouse-data:/var/lib/clickhouse \
  clickhouse/clickhouse-server:26.2

# 3. Start Backend
podman run -d --name backend --network observability-net \
  -p 9090:9090 \
  -e GO_ENV=production \
  -e PORT=9090 \
  -e MYSQL_HOST=mysql -e MYSQL_USERNAME=root -e MYSQL_PASSWORD=root123 \
  -e CLICKHOUSE_HOST=clickhouse -e CLICKHOUSE_USERNAME=default -e CLICKHOUSE_PASSWORD=clickhouse123 \
  -e JWT_SECRET=your_super_secret_jwt_key_change_this_in_production \
  ghcr.io/optikk-org/optikk-lens:latest

# 4. Start Frontend
podman run -d --name frontend --network observability-net \
  -p 8443:8443 \
  -e BACKEND_URL=http://backend:9090 \
  ghcr.io/optikk-org/optikk-lens-frontend:latest

# View logs
podman logs backend
podman logs frontend

# Access the UI
# https://localhost:8443 (self-signed certificate warning expected)
```

---

## Local Development

### Build the Backend Locally

```bash
# From the root directory
go build -v ./cmd/server

# Run the binary
./cmd/server/server
```

Requires MySQL and ClickHouse running with matching environment variables.

### Project Structure

```
optikk-backend/
├── cmd/server/              # Entry point
├── internal/
│   ├── config/              # Config & env loading
│   ├── database/            # DB pools, wrappers, helpers
│   ├── platform/            # HTTP server, middleware, auth
│   │   ├── otlp/            # OTLP ingestion (HTTP/gRPC)
│   │   ├── middleware/      # Auth, rate-limiting, CORS
│   │   └── ingest/          # Batching & queuing
│   └── modules/             # Feature modules
│       ├── spans/           # Trace/span queries & detail endpoints
│       ├── services/        # Service topology & latency
│       ├── logs/            # Log search & filtering
│       ├── overview/        # Dashboard aggregations
│       ├── infrastructure/  # Node/pod/container metrics
│       ├── saturation/      # Queue depth & consumer lag
│       └── ...
└── db/                      # Schema SQL files
    ├── mysql_schema.sql
    └── clickhouse_schema.sql
```

---

## API Documentation

### OTLP Ingestion (Telemetry)

OpenTelemetry Protocol endpoints for sending traces, logs, and metrics:

```bash
# Send traces (HTTP)
curl -X POST http://localhost:8080/v1/traces \
  -H "Content-Type: application/json" \
  -H "api-key: <team-api-key>" \
  -d @traces.json

# Send logs (HTTP)
curl -X POST http://localhost:8080/v1/logs \
  -H "Content-Type: application/json" \
  -H "api-key: <team-api-key>" \
  -d @logs.json

# Send metrics (HTTP)
curl -X POST http://localhost:8080/v1/metrics \
  -H "Content-Type: application/json" \
  -H "api-key: <team-api-key>" \
  -d @metrics.json
```

### Query Endpoints

#### Traces

- `GET /api/v1/traces` — List traces with filtering & pagination
- `GET /api/v1/traces/:traceId` — Get full trace detail
- `GET /api/v1/traces/:traceId/spans/:spanId` — Get span detail with attributes
- `GET /api/v1/traces/:traceId/span-events` — Get span events & exceptions
- `GET /api/v1/traces/:traceId/critical-path` — Get longest root→leaf span chain
- `GET /api/v1/traces/:traceId/error-path` — Get error span chain
- `GET /api/v1/traces/:traceId/span-self-times` — Get self-time per span

#### Services

- `GET /api/v1/services` — List all services
- `GET /api/v1/services/:serviceName` — Get service metrics & health
- `GET /api/v1/services/:serviceName/upstream-downstream` — Get service dependencies

#### Metrics

- `GET /api/v1/spans/top-slow-operations` — Slowest operations by p99 latency
- `GET /api/v1/spans/top-error-operations` — Operations with highest error rate
- `GET /api/v1/spans/service-scorecard` — Per-service stats (RPS, error %, p95)
- `GET /api/v1/spans/exception-rate-by-type` — Exception frequency

#### Logs

- `GET /api/v1/logs/search` — Full-text search with filtering
- `GET /api/v1/logs/aggregations` — Log field cardinality & top values

---

## Authentication

The backend uses JWT tokens for API access. Users/teams authenticate via:

1. **API Key Ingestion** — OTLP telemetry endpoints accept `api-key` header
2. **User Login** — Web UI login returns a JWT token (stored in httpOnly cookie)
3. **JWT Verification** — All `/api/v1/*` endpoints require a valid JWT token

---

## Performance & Scaling

- **ClickHouse**: Optimized for high-throughput telemetry ingestion; horizontal scaling via native replication
- **MySQL**: Stores config, users, teams; use managed services (RDS, CloudSQL) for HA
- **Rate Limiting**: In-process token bucket; can be scaled with Redis
- **Batched Ingest**: Spans/logs/metrics batched by team; Kafka optional for larger deployments

---

## Development & Contributing

### Building from Source

```bash
# Install Go 1.24+
# From the root directory

# Sync workspace dependencies
go work sync

# Build the server
go build -v ./cmd/server

# Run tests
go test ./...

# Run linter
go vet ./...
```

### Project Modules

Each feature module follows a consistent pattern:
- `models.go` — Data types & DTOs
- `repository.go` — Database queries
- `service.go` — Business logic
- `handler.go` — HTTP endpoints
- `module.go` — Dependency injection & route registration
- `defaults.go` — Dashboard config registration

---

## License

This project is part of Optikk. See LICENSE file for details.
