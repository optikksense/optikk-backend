# Observability Backend (Go + ClickHouse)

Go/Gin backend for the observability platform. Handles REST API queries, JWT auth, multi-tenant telemetry ingestion (OTLP/HTTP), and ClickHouse persistence.

## Build and Push

```bash
# Build (from project root)
docker build -t ramantayal12/observability-backend:latest .

# Push to Docker Hub
docker login
docker push ramantayal12/observability-backend:latest
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `127.0.0.1` | ClickHouse host |
| `CLICKHOUSE_PORT` | `9000` | ClickHouse port |
| `CLICKHOUSE_DATABASE` | `observability` | Database name |
| `CLICKHOUSE_USERNAME` | `default` | Database user |
| `CLICKHOUSE_PASSWORD` | `clickhouse123` | Database password |
| `MYSQL_HOST` | `127.0.0.1` | MariaDB host (for identity/alerts) |
| `MYSQL_DATABASE` | `observability` | Database name |
| `MYSQL_USERNAME` | `root` | Database user |
| `MYSQL_PASSWORD` | `root123` | Database password |
| `JWT_SECRET` | *(built-in default)* | JWT secret (set for production) |
| `PORT` | `8080` | HTTP server port |

---

## VM Deployment with Podman

```bash
# Install Podman (Debian/Ubuntu)
sudo apt-get install -y podman

# Install Podman (RHEL/Fedora)
sudo dnf install -y podman
```

### Deploy Full Stack

```bash
# 1. Start ClickHouse
podman run -d --name clickhouse \
  -p 9000:9000 -p 8123:8123 \
  -e CLICKHOUSE_DB=observability \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=clickhouse123 \
  -v clickhouse-data:/var/lib/clickhouse \
  clickhouse/clickhouse-server:24.3

# 2. Start Backend
podman run -d --name backend \
  -p 8080:8080 \
  -e CLICKHOUSE_HOST=127.0.0.1 -e CLICKHOUSE_DATABASE=observability \
  -e CLICKHOUSE_USERNAME=default -e CLICKHOUSE_PASSWORD=clickhouse123 \
  docker.io/ramantayal12/observability-backend:latest

# 3. Start Frontend
podman run -d --name frontend \
  -p 8443:8443 \
  -e BACKEND_URL=http://127.0.0.1:8080 \
  docker.io/ramantayal12/observability-frontend:latest
```

Access at `https://localhost:8443` (self-signed cert warning expected).

**Production:** Add `-e JWT_SECRET=your-secure-secret` to backend, mount custom certs to frontend:
```bash
-v /path/to/cert.pem:/etc/nginx/ssl/cert.pem:ro \
-v /path/to/key.pem:/etc/nginx/ssl/key.pem:ro
```

### Verify

```bash
podman ps
curl http://localhost:8080/api/health
curl -k https://localhost:8443  # Frontend (self-signed cert)
```

### Management

```bash
# Logs
podman logs -f backend

# Stop/Start
podman stop frontend backend clickhouse
podman start clickhouse backend frontend

# Remove (data persists in volumes)
podman rm -f frontend backend clickhouse
podman volume rm clickhouse-data  # WARNING: deletes data
```

---

## Local Development

```bash
# Requires Go 1.23+, MariaDB on port 3306, and ClickHouse on port 9000
go run ./cmd/server
```
Schema migration runs automatically from `migrations/` on startup.

### Seed Demo Data

```bash
# Create user/team and seed telemetry data via OTLP
python3 scripts/seed_single_user_metrics.py

# Custom user/team
python3 scripts/seed_single_user_metrics.py \
  --email user@example.com \
  --name "John Doe" \
  --team-name "My Team"

# Reset existing data before seeding
python3 scripts/seed_single_user_metrics.py --reset
```

The script creates a user and team via `/api/signup`, then ingests synthetic metrics, logs, and traces via OTLP HTTP endpoints.

## Architecture (Modular)

- `internal/platform/handlers/base.go` contains shared HTTP/query/typing utility helpers used by all modules.
- Domain handlers are split into modules under `internal/modules/*`:
  - `identity`
  - `alerts`
  - `health`
  - `deployments`
  - `logs`
  - `traces`
  - `metrics`
  - `insights`
  - `ai`
- All non-identity modules share a common dependency struct (`DB` + `GetTenant`) via `internal/modules/common/deps.go`, reducing repeated boilerplate.
- Identity database access is segregated into table repositories:
  - `UserTableRepository`
  - `TeamTableRepository`
  - `UserTeamTableRepository`
- App wiring composes these repositories through a `TableProvider` in `internal/modules/identity/mysql_store.go`.

This makes per-table DB swaps explicit: you can replace only one repository implementation (for example `TeamTableRepository`) without changing handlers/routes.

## Database Configuration

- **ClickHouse**: Used for logs, metrics, and traces (spans) tables - optimized for time-series data
- **MariaDB/MySQL**: Used for identity, alerts, health checks, deployments, and other relational data

## Telemetry Ingestion API
- **HTTP**: `POST /otlp/v1/metrics`, `POST /otlp/v1/logs`, and `POST /otlp/v1/traces` on port `8080`
