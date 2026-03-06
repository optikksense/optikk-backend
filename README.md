# Optikk Backend

The Go/Gin backend service for the Optikk observability platform. It provides a REST API, JWT authentication, and multi-tenant telemetry ingestion (OTLP/HTTP) backed by MySQL and ClickHouse.

## Prerequisites

Before running the application, you must set up the required databases (MySQL and ClickHouse) and execute their respective schema scripts to initialize the tables.

### 1. Execute Database Schemas

#### **MySQL** (Used for user auth, teams, alerts, and dashboard configs):
Log into the MySQL container and create the database if it doesn't exist:
```bash
docker exec -it mariadb mariadb -u root -proot123 -e "CREATE DATABASE IF NOT EXISTS observability;"
```

Execute the table creation commands individually:
```bash
# Create users table
docker exec -i mysql mysql -u root -proot123 observability -e "CREATE TABLE IF NOT EXISTS users (id BIGINT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE, password_hash VARCHAR(255), name VARCHAR(100) NOT NULL, avatar_url VARCHAR(255), role VARCHAR(20) NOT NULL DEFAULT 'member', teams JSON NOT NULL DEFAULT ('[]'), active TINYINT(1) NOT NULL DEFAULT 1, last_login_at DATETIME NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NULL, INDEX idx_user_email (email));"

# Create teams table
docker exec -i mariadb mariadb -u root -proot123 observability -e "CREATE TABLE IF NOT EXISTS teams (id BIGINT AUTO_INCREMENT PRIMARY KEY, org_name VARCHAR(100) NOT NULL, name VARCHAR(100) NOT NULL, slug VARCHAR(50), description VARCHAR(500), active TINYINT(1) NOT NULL DEFAULT 1, color VARCHAR(50), icon VARCHAR(100), api_key VARCHAR(64) NOT NULL UNIQUE, retention_days INT NOT NULL DEFAULT 30, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NULL, INDEX idx_team_api_key (api_key), UNIQUE KEY uq_team_org_name (org_name, name));"
```

#### **ClickHouse** (Used for telemetry data: spans, logs, metrics):
Execute the ClickHouse table creation commands individually:
```bash
# Create metrics table
docker exec -i clickhouse clickhouse-client -u default --password clickhouse123 -q "CREATE TABLE IF NOT EXISTS observability.metrics (team_id LowCardinality(String), env LowCardinality(String) DEFAULT 'default', metric_name LowCardinality(String), metric_type LowCardinality(String), temporality LowCardinality(String) DEFAULT 'Unspecified', is_monotonic Bool CODEC(T64, ZSTD(1)), unit LowCardinality(String) DEFAULT '', description LowCardinality(String) DEFAULT '', resource_fingerprint UInt64 CODEC(Delta(8), ZSTD(1)), timestamp DateTime64(3) CODEC(DoubleDelta, LZ4), value Float64 CODEC(Gorilla, ZSTD(1)), hist_sum Float64 CODEC(Gorilla, ZSTD(1)), hist_count UInt64 CODEC(T64, ZSTD(1)), hist_buckets Array(Float64) CODEC(ZSTD(1)), hist_counts Array(UInt64) CODEC(T64, ZSTD(1)), attributes JSON(max_dynamic_paths=100) CODEC(ZSTD(1)), service LowCardinality(String) MATERIALIZED attributes.\`service.name\`::String, host LowCardinality(String) MATERIALIZED attributes.\`host.name\`::String, environment LowCardinality(String) MATERIALIZED attributes.\`deployment.environment\`::String, k8s_namespace LowCardinality(String) MATERIALIZED attributes.\`k8s.namespace.name\`::String, http_method LowCardinality(String) MATERIALIZED attributes.\`http.method\`::String, http_status_code UInt16 MATERIALIZED attributes.\`http.status_code\`::UInt16, has_error Bool MATERIALIZED attributes.\`error\`::Bool, INDEX idx_service service TYPE set(200) GRANULARITY 1, INDEX idx_host host TYPE bloom_filter GRANULARITY 1, INDEX idx_environment environment TYPE set(10) GRANULARITY 1, INDEX idx_k8s_namespace k8s_namespace TYPE set(100) GRANULARITY 1, INDEX idx_http_method http_method TYPE set(20) GRANULARITY 1, INDEX idx_http_status_code http_status_code TYPE minmax GRANULARITY 1, INDEX idx_has_error has_error TYPE set(2) GRANULARITY 1, INDEX idx_fingerprint resource_fingerprint TYPE bloom_filter GRANULARITY 4) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(timestamp) ORDER BY (team_id, metric_name, service, environment, temporality, timestamp, resource_fingerprint) TTL toDateTime(timestamp) + INTERVAL 365 DAY DELETE SETTINGS index_granularity = 8192, enable_mixed_granularity_parts = 1;"
```

*(Adjust user and password parameters as needed for your database connections).*

## Running the Application with Docker

Once the databases are set up and the schemas are applied, you can run the backend application using Docker.

### 1. Build the Docker Image
From the root of the project directory, run:
```bash
docker build -t optikk-backend:latest .
```

### 2. Run the Docker Container
Run the container and expose port `8080`. You will need to pass the appropriate environment variables so the container can connect to your databases. 

*Note: If your databases are running locally on your host machine (e.g., via Docker Desktop), use `host.docker.internal` for the hosts.*

```bash
docker run -d \
  --name optikk-backend \
  -p 8080:8080 \
  -e GO_ENV=production \
  -e PORT=8080 \
  -e MYSQL_HOST=host.docker.internal \
  -e MYSQL_PORT=3306 \
  -e MYSQL_DATABASE=observability \
  -e MYSQL_USERNAME=root \
  -e MYSQL_PASSWORD=root123 \
  -e CLICKHOUSE_HOST=host.docker.internal \
  -e CLICKHOUSE_PORT=9000 \
  -e CLICKHOUSE_DATABASE=observability \
  -e CLICKHOUSE_USERNAME=default \
  -e CLICKHOUSE_PASSWORD=clickhouse123 \
  -e ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173 \
  -e JWT_SECRET=your_super_secret_jwt_key_that_is_long_enough \
  -e REDIS_ENABLED=false \
  -e KAFKA_ENABLED=false \
  optikk-backend:latest
```

**Environment Variables Overview:**
- `GO_ENV=production`: Enforces secure defaults (requires non-default passwords and secrets).
- `MYSQL_*`: Credentials to connect to the MySQL database.
- `CLICKHOUSE_*`: Credentials to connect to the ClickHouse database.
- `ALLOWED_ORIGINS`: Comma-separated UI origins allowed by CORS (for example `http://localhost:3000,http://localhost:5173`).
- `JWT_SECRET`: A secure randomly generated secret used for signing JWT auth tokens.
- `REDIS_ENABLED` / `KAFKA_ENABLED`: Toggle depending on whether you're using Redis for pub/sub (SSE) and token blacklisting, and Kafka for telemetry queuing. (Set to `true` and define target hosts/ports if using them).

### 3. Get dabases Up Before

#### Run MySQl
```bash
docker run -d \
  --name mysql \
  -e MYSQL_ROOT_PASSWORD=root123 \     
  -e MYSQL_DATABASE=observability \
  -p 3306:3306 \
  mysql:latest
```

#### Run Clickhouse 
```bash
docker run -d \
  --name clickhouse \
  -p 8123:8123 \
  -p 9000:9000 \
  --ulimit nofile=262144:262144 \
  -e CLICKHOUSE_DB=observability \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=clickhouse123 \
  clickhouse/clickhouse-server:26.2
```

### Data Dropping
# Connect to ClickHouse
docker exec -it clickhouse clickhouse-client

# Drop entire database (drops all tables inside)
DROP DATABASE IF EXISTS observability;

# Recreate fresh
CREATE DATABASE IF NOT EXISTS observability;
