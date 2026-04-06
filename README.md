# Optikk Lens Backend (Go/Gin)

Optikk Lens Backend is the high-performance core of the Optikk observability platform. Built with Go and the Gin web framework, it provides a unified REST API for telemetry data (traces, logs, and metrics), manages multi-tenant authentication, and orchestrates real-time data ingestion via the OpenTelemetry Protocol (OTLP).

## Core Architecture

The backend follows a modular monolith architecture, segregating concerns between the ingestion pipeline, data persistence, and the consumption API.

### High-Level Components

*   **OTLP Ingestion Engine**: Receives telemetry via gRPC (port `4317`), publishes rows to **Redis Streams**, and background workers batch to ClickHouse and fan out live tail streams.
*   **Module System**: The domain logic is subdivided into decoupled modules (see `internal/modules`):
    *   `auth`: JWT-based user session and tenant management.
    *   `traces` & `logs`: Query logic for telemetry data.
    *   `infrastructure`: Kubernetes and cloud resource utilization metrics.
    *   `ai`: LLM and AI-agent monitoring integration.
    *   `apm`: Pre-aggregated service-level performance metrics (RED).
*   **Data Tier**:
    *   **MariaDB**: Stores relational data (users, teams, dashboard configurations, and metadata).
    *   **ClickHouse**: The primary analytical store for petabyte-scale telemetry storage.
    *   **Redis**: Query caching, sessions, OTLP ingest streams, and live-tail fan-out.

## Project Structure

```text
optikk-backend/
├── cmd/server          # Main entry point (Gin server)
├── db/                 # Database schema definitions (SQL)
├── internal/
│   ├── app/            # Application lifecycle and dependency injection
│   ├── ingestion/      # OTLP → Redis Streams + CH consumers
│   ├── infra/          # Infrastructure abstractions (DB, CH, Redis)
│   ├── modules/        # Domain-specific business logic
│   └── config/         # YAML-based configuration management
└── README.md
```

## Local Development

### 1. Prerequisites

You'll need a running local infrastructure (MariaDB, ClickHouse, etc.). Use the central deployment guide:
👉 [**Full Stack Local Deployment Guide**](../deploy/README.md)

### 2. Configuration

Copy the default configuration and adjust your connection strings:
```bash
cp config.yml.example config.yml
```

### 3. Build & Run

Ensure you have Go 1.22+ installed.

```bash
# Install dependencies
go mod download

# Run directly
go run cmd/server/main.go

# Or build the binary
go build -o bin/server cmd/server/main.go
./bin/server
```

## API Documentation

The backend serves an Internal API for the Optikk Frontend.
- Port: `19090` (default)
- Health Check: `GET /api/v1/health`
- OTLP gRPC: `Port 4317`

## Technical Details

- **Framework**: [Gin Gonic](https://github.com/gin-gonic/gin)
- **Persistence Layer**: [SQLx](https://github.com/jmoiron/sqlx) for MySQL/MariaDB, and [ClickHouse Go Client](https://github.com/ClickHouse/clickhouse-go).
- **Caching**: [Go-Redis](https://github.com/redis/go-redis).
- **Ingestion**: OTLP rows via Redis Streams; ClickHouse flush in `streamworkers`.
