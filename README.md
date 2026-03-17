# Optikk Lens Backend

The Go/Gin backend service for the Optikk observability platform. It provides a REST API for telemetry data (traces, logs, metrics), JWT authentication, multi-tenant support, and real-time ingestion via OpenTelemetry Protocol (OTLP). Data is persisted in MySQL (config, users) and ClickHouse (telemetry).

## Quick Start

### Prerequisites

- Docker and Docker Compose, or Podman
- MySQL 8.0+ (or MariaDB 11.0+)
- ClickHouse 26.2+
- An initialized database schema (see [Database Setup](#database-setup))

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
  -e SESSION_COOKIE_NAME=optikk_session \
  -e SESSION_COOKIE_SECURE=false \
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
| `SESSION_COOKIE_NAME` | Cookie name for the server-side session | `optikk_session` |
| `SESSION_COOKIE_SECURE` | Mark the session cookie as HTTPS-only | `false` |
| `SESSION_IDLE_TIMEOUT_MS` | Idle timeout for authenticated sessions | `7200000` |
| `SESSION_LIFETIME_MS` | Absolute session lifetime | `86400000` |
| `REDIS_ENABLED` | Enable Redis for query caching and session persistence | `false` |
| `REDIS_HOST` | Redis hostname (if enabled) | `localhost` |
| `REDIS_PORT` | Redis port (if enabled) | `6379` |
| `KAFKA_ENABLED` | Enable Kafka for batched ingest queuing | `false` |
| `KAFKA_BROKERS` | Kafka broker addresses (if enabled) | `localhost:9092` |


## Local Development

### Build the Backend Locally

```bash
# From the root directory
go build -v ./cmd/server

# Run the binary
./cmd/server/server
```

Requires MySQL and ClickHouse running with matching environment variables.
