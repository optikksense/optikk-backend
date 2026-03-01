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
| `PORT` | `8080` | HTTP server port |
| `MYSQL_HOST` | `127.0.0.1` | MariaDB host (for identity/alerts) |
| `MYSQL_PORT` | `3306` | MariaDB port |
| `MYSQL_DATABASE` | `observability` | Database name |
| `MYSQL_USERNAME` | `root` | Database user |
| `MYSQL_PASSWORD` | `root123` | Database password |
| `CLICKHOUSE_HOST` | `127.0.0.1` | ClickHouse host |
| `CLICKHOUSE_PORT` | `9000` | ClickHouse port |
| `CLICKHOUSE_DATABASE` | `observability` | Database name |
| `CLICKHOUSE_USERNAME` | `default` | Database user |
| `CLICKHOUSE_PASSWORD` | `clickhouse123` | Database password |
| `JWT_SECRET` | *(built-in default)* | JWT secret (must set for production) |
| `JWT_EXPIRATION_MS` | `86400000` | JWT expiration (1 day) |
| `QUEUE_BATCH_SIZE` | `500` | Batch size for queue processing |
| `QUEUE_FLUSH_INTERVAL_MS`| `2000` | Flush interval for queues (ms) |
| `KAFKA_ENABLED` | `true` | Enable Kafka for high-throughput ingestion |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker list (comma-separated) |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `ALLOWED_ORIGINS` | *(empty)* | CORS allowed origins (comma-separated) |
| `MAX_MYSQL_OPEN_CONNS` | `50` | Max open connections to MySQL |
| `MAX_MYSQL_IDLE_CONNS` | `25` | Max idle connections to MySQL |
| `DEFAULT_RETENTION_DAYS` | `30` | Default data retention |
| `APP_REGION` | `us-east-1` | App region for multi-region scope |
| `GO_ENV` | *(empty)* | Set `production` to enforce secure secrets |

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
# 0. Create network
podman network create observability

# 1. Start MariaDB
podman run -d --name mariadb --network observability \
  -p 3306:3306 \
  -e MARIADB_ROOT_PASSWORD=root123 \
  -e MARIADB_DATABASE=observability \
  -v mariadb-data:/var/lib/mysql \
  mariadb:11

# 2. Start ClickHouse
podman run -d --name clickhouse --network observability \
  -p 9000:9000 -p 8123:8123 \
  -e CLICKHOUSE_DB=observability \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=clickhouse123 \
  -v clickhouse-data:/var/lib/clickhouse \
  clickhouse/clickhouse-server:24.3

# 3. Start Kafka (KRaft, no Zookeeper)
podman run -d --name kafka --network observability \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
  apache/kafka:latest

# 4. Start Backend
podman run -d --name backend --network observability \
  -p 8080:8080 \
  -e CLICKHOUSE_HOST=clickhouse -e CLICKHOUSE_DATABASE=observability \
  -e CLICKHOUSE_USERNAME=default -e CLICKHOUSE_PASSWORD=clickhouse123 \
  -e MYSQL_HOST=mariadb -e MYSQL_DATABASE=observability \
  -e MYSQL_USERNAME=root -e MYSQL_PASSWORD=root123 \
  -e KAFKA_BROKERS=kafka:9092 \
  docker.io/ramantayal12/observability-backend:latest

# 5. Start Frontend
podman run -d --name frontend --network observability \
  -p 8443:8443 \
  -e BACKEND_URL=http://backend:8080 \
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
podman stop frontend backend kafka clickhouse mariadb
podman start mariadb clickhouse kafka backend frontend

# Remove (data persists in volumes)
podman rm -f frontend backend kafka clickhouse mariadb
podman volume rm clickhouse-data mariadb-data  # WARNING: deletes data
```

---

## Local Development

If you are running the backing services (MariaDB, ClickHouse, Kafka, Redis) on their default ports locally, you can run the server directly:

```bash
# Requires Go 1.23+ and backing services
go run ./cmd/server
```

If your backing services are hosted elsewhere or use custom ports, you must populate the required environment variables inline or via an export script:

```bash
PORT=8080 \
MYSQL_HOST=127.0.0.1 MYSQL_PORT=3306 \
CLICKHOUSE_HOST=127.0.0.1 CLICKHOUSE_PORT=9000 \
KAFKA_ENABLED=true KAFKA_BROKERS=localhost:9092 \
REDIS_HOST=localhost REDIS_PORT=6379 \
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
