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
docker exec -i mariadb mariadb -u root -proot123 observability -e "CREATE TABLE IF NOT EXISTS users (id BIGINT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE, password_hash VARCHAR(255), name VARCHAR(100) NOT NULL, avatar_url VARCHAR(255), role VARCHAR(20) NOT NULL DEFAULT 'member', teams JSON NOT NULL DEFAULT ('[]'), active TINYINT(1) NOT NULL DEFAULT 1, last_login_at DATETIME NULL, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NULL, INDEX idx_user_email (email));"

# Create teams table
docker exec -i mariadb mariadb -u root -proot123 observability -e "CREATE TABLE IF NOT EXISTS teams (id BIGINT AUTO_INCREMENT PRIMARY KEY, org_name VARCHAR(100) NOT NULL, name VARCHAR(100) NOT NULL, slug VARCHAR(50), description VARCHAR(500), active TINYINT(1) NOT NULL DEFAULT 1, color VARCHAR(50), icon VARCHAR(100), api_key VARCHAR(64) NOT NULL UNIQUE, retention_days INT NOT NULL DEFAULT 30, slack_webhook_url VARCHAR(255), dashboard_configs JSON NULL, data_ingested_kb BIGINT NOT NULL DEFAULT 0, created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME NULL, INDEX idx_team_api_key (api_key), UNIQUE KEY uq_team_org_name (org_name, name));"
```

*Note: The `dashboard_configs` JSON column in the `teams` table serves as the primary source of truth for page and dashboard layouts. It is automatically initialized with the full JSON payload of all default system configurations when a new team is created. Legacy `default_page_config` tables are no longer used.*

#### **ClickHouse** (Used for telemetry data: spans, logs, metrics):
First, create the database, then execute the table creation commands. See `db/clickhouse_schema.sql` for full schemas.

```bash
# Create database
docker exec -i clickhouse clickhouse-client -u default --password clickhouse123 -q "CREATE DATABASE IF NOT EXISTS observability;"

# Create spans table
docker exec -i clickhouse clickhouse-client -u default --password clickhouse123 < db/clickhouse_schema.sql

# Or create tables individually:

# Create spans table
docker exec -i clickhouse clickhouse-client -u default --password clickhouse123 -q "CREATE TABLE IF NOT EXISTS observability.spans (ts_bucket_start UInt64 CODEC(DoubleDelta, LZ4), team_id LowCardinality(String) CODEC(ZSTD(1)), timestamp DateTime64(9) CODEC(DoubleDelta, LZ4), trace_id FixedString(32) CODEC(ZSTD(1)), span_id FixedString(16) CODEC(ZSTD(1)), parent_span_id FixedString(16) CODEC(ZSTD(1)), trace_state String CODEC(ZSTD(1)), flags UInt32 CODEC(T64, ZSTD(1)), name LowCardinality(String) CODEC(ZSTD(1)), kind Int8 CODEC(T64, ZSTD(1)), kind_string LowCardinality(String) CODEC(ZSTD(1)), duration_nano UInt64 CODEC(T64, ZSTD(1)), has_error Bool CODEC(T64, ZSTD(1)), is_remote Bool CODEC(T64, ZSTD(1)), status_code Int16 CODEC(T64, ZSTD(1)), status_code_string LowCardinality(String) CODEC(ZSTD(1)), status_message String CODEC(ZSTD(1)), http_url LowCardinality(String) CODEC(ZSTD(1)), http_method LowCardinality(String) CODEC(ZSTD(1)), http_host LowCardinality(String) CODEC(ZSTD(1)), external_http_url LowCardinality(String) CODEC(ZSTD(1)), external_http_method LowCardinality(String) CODEC(ZSTD(1)), response_status_code LowCardinality(String) CODEC(ZSTD(1)), attributes JSON(max_dynamic_paths = 50) CODEC(ZSTD(1)), events Array(String) CODEC(ZSTD(2)), links String CODEC(ZSTD(1)), exception_type LowCardinality(String) CODEC(ZSTD(1)), exception_message String CODEC(ZSTD(1)), exception_stacktrace String CODEC(ZSTD(1)), exception_escaped Bool CODEC(T64, ZSTD(1)), service_name LowCardinality(String) MATERIALIZED attributes.\`service.name\`::String CODEC(ZSTD(1)), operation_name LowCardinality(String) ALIAS name, start_time DateTime64(9) ALIAS timestamp, duration_ms Float64 ALIAS duration_nano / 1000000.0, status LowCardinality(String) ALIAS status_code_string, http_status_code UInt16 ALIAS toUInt16OrZero(response_status_code), is_root UInt8 ALIAS if((parent_span_id = '') OR (parent_span_id = '0000000000000000'), 1, 0), parent_service_name LowCardinality(String) ALIAS '', peer_address LowCardinality(String) ALIAS CAST(attributes.\`peer.address\`, 'String'), mat_http_route LowCardinality(String) MATERIALIZED attributes.\`http.route\`::String CODEC(ZSTD(1)), mat_http_status_code LowCardinality(String) MATERIALIZED attributes.\`http.status_code\`::String CODEC(ZSTD(1)), mat_http_target LowCardinality(String) MATERIALIZED attributes.\`http.target\`::String CODEC(ZSTD(1)), mat_http_scheme LowCardinality(String) MATERIALIZED attributes.\`http.scheme\`::String CODEC(ZSTD(1)), mat_db_system LowCardinality(String) MATERIALIZED attributes.\`db.system\`::String CODEC(ZSTD(1)), mat_db_name LowCardinality(String) MATERIALIZED attributes.\`db.name\`::String CODEC(ZSTD(1)), mat_db_operation LowCardinality(String) MATERIALIZED attributes.\`db.operation\`::String CODEC(ZSTD(1)), mat_db_statement String MATERIALIZED attributes.\`db.statement\`::String CODEC(ZSTD(1)), mat_rpc_system LowCardinality(String) MATERIALIZED attributes.\`rpc.system\`::String CODEC(ZSTD(1)), mat_rpc_service LowCardinality(String) MATERIALIZED attributes.\`rpc.service\`::String CODEC(ZSTD(1)), mat_rpc_method LowCardinality(String) MATERIALIZED attributes.\`rpc.method\`::String CODEC(ZSTD(1)), mat_rpc_grpc_status_code LowCardinality(String) MATERIALIZED attributes.\`rpc.grpc.status_code\`::String CODEC(ZSTD(1)), mat_messaging_system LowCardinality(String) MATERIALIZED attributes.\`messaging.system\`::String CODEC(ZSTD(1)), mat_messaging_operation LowCardinality(String) MATERIALIZED attributes.\`messaging.operation\`::String CODEC(ZSTD(1)), mat_messaging_destination LowCardinality(String) MATERIALIZED attributes.\`messaging.destination\`::String CODEC(ZSTD(1)), mat_peer_service LowCardinality(String) MATERIALIZED attributes.\`peer.service\`::String CODEC(ZSTD(1)), mat_net_peer_name LowCardinality(String) MATERIALIZED attributes.\`net.peer.name\`::String CODEC(ZSTD(1)), mat_net_peer_port LowCardinality(String) MATERIALIZED attributes.\`net.peer.port\`::String CODEC(ZSTD(1)), mat_exception_type LowCardinality(String) MATERIALIZED attributes.\`exception.type\`::String CODEC(ZSTD(1)), mat_host_name LowCardinality(String) MATERIALIZED attributes.\`host.name\`::String CODEC(ZSTD(1)), mat_k8s_pod_name LowCardinality(String) MATERIALIZED attributes.\`k8s.pod.name\`::String CODEC(ZSTD(1)), INDEX idx_service_name service_name TYPE bloom_filter(0.01) GRANULARITY 4, INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 4, INDEX idx_span_name name TYPE bloom_filter(0.01) GRANULARITY 4, INDEX idx_mat_http_route mat_http_route TYPE bloom_filter(0.01) GRANULARITY 4, INDEX idx_mat_http_status_code mat_http_status_code TYPE bloom_filter(0.01) GRANULARITY 4, INDEX idx_mat_db_system mat_db_system TYPE bloom_filter(0.01) GRANULARITY 4, INDEX idx_mat_db_name mat_db_name TYPE bloom_filter(0.01) GRANULARITY 4, INDEX idx_mat_rpc_service mat_rpc_service TYPE bloom_filter(0.01) GRANULARITY 4, INDEX idx_mat_peer_service mat_peer_service TYPE bloom_filter(0.01) GRANULARITY 4, INDEX idx_mat_exception_type mat_exception_type TYPE bloom_filter(0.01) GRANULARITY 4, INDEX idx_mat_host_name mat_host_name TYPE bloom_filter(0.01) GRANULARITY 4, INDEX idx_mat_k8s_pod_name mat_k8s_pod_name TYPE bloom_filter(0.01) GRANULARITY 4) ENGINE = MergeTree() PARTITION BY toYYYYMM(timestamp) ORDER BY (team_id, ts_bucket_start, service_name, name, timestamp) TTL toDate(timestamp) + INTERVAL 14 DAY TO VOLUME 'warm', toDate(timestamp) + INTERVAL 30 DAY DELETE SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;"

# Create logs table
docker exec -i clickhouse clickhouse-client -u default --password clickhouse123 -q "CREATE TABLE IF NOT EXISTS observability.logs (team_id LowCardinality(String) CODEC(ZSTD(1)), ts_bucket_start UInt32 CODEC(Delta(4), LZ4), timestamp UInt64 CODEC(DoubleDelta, LZ4), observed_timestamp UInt64 CODEC(DoubleDelta, LZ4), id String CODEC(ZSTD(1)), trace_id String CODEC(ZSTD(1)), span_id String CODEC(ZSTD(1)), trace_flags UInt32 DEFAULT 0, severity_text LowCardinality(String) CODEC(ZSTD(1)), severity_number UInt8 DEFAULT 0, body String CODEC(ZSTD(2)), attributes_string Map(LowCardinality(String), String) CODEC(ZSTD(1)), attributes_number Map(LowCardinality(String), Float64) CODEC(ZSTD(1)), attributes_bool Map(LowCardinality(String), Bool) CODEC(ZSTD(1)), resource JSON(max_dynamic_paths=100) CODEC(ZSTD(1)), resource_fingerprint String CODEC(ZSTD(1)), scope_name String CODEC(ZSTD(1)), scope_version String CODEC(ZSTD(1)), scope_string Map(LowCardinality(String), String) CODEC(ZSTD(1)), service LowCardinality(String) MATERIALIZED resource.\`service.name\`::String, host LowCardinality(String) MATERIALIZED resource.\`host.name\`::String, pod LowCardinality(String) MATERIALIZED resource.\`k8s.pod.name\`::String, container LowCardinality(String) MATERIALIZED resource.\`k8s.container.name\`::String, environment LowCardinality(String) MATERIALIZED resource.\`deployment.environment\`::String, INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1, INDEX idx_span_id span_id TYPE bloom_filter(0.01) GRANULARITY 1, INDEX idx_body body TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1, INDEX idx_severity severity_text TYPE set(10) GRANULARITY 1, INDEX idx_service service TYPE set(200) GRANULARITY 1, INDEX idx_host host TYPE bloom_filter(0.01) GRANULARITY 1) ENGINE = MergeTree() PARTITION BY toYYYYMM(toDateTime(ts_bucket_start)) ORDER BY (team_id, ts_bucket_start, service, timestamp) TTL toDateTime(ts_bucket_start) + INTERVAL 30 DAY DELETE SETTINGS index_granularity = 8192;"

# Create metrics table
docker exec -i clickhouse clickhouse-client -u default --password clickhouse123 -q "CREATE TABLE IF NOT EXISTS observability.metrics (team_id LowCardinality(String), env LowCardinality(String) DEFAULT 'default', metric_name LowCardinality(String), metric_type LowCardinality(String), temporality LowCardinality(String) DEFAULT 'Unspecified', is_monotonic Bool CODEC(T64, ZSTD(1)), unit LowCardinality(String) DEFAULT '', description LowCardinality(String) DEFAULT '', resource_fingerprint UInt64 CODEC(ZSTD(3)), timestamp DateTime64(3) CODEC(DoubleDelta, LZ4), value Float64 CODEC(Gorilla, ZSTD(1)), hist_sum Float64 CODEC(Gorilla, ZSTD(1)), hist_count UInt64 CODEC(T64, ZSTD(1)), hist_buckets Array(Float64) CODEC(ZSTD(1)), hist_counts Array(UInt64) CODEC(T64, ZSTD(1)), attributes JSON(max_dynamic_paths=100) CODEC(ZSTD(1)), service LowCardinality(String) MATERIALIZED attributes.\`service.name\`::String, host LowCardinality(String) MATERIALIZED attributes.\`host.name\`::String, environment LowCardinality(String) MATERIALIZED attributes.\`deployment.environment\`::String, k8s_namespace LowCardinality(String) MATERIALIZED attributes.\`k8s.namespace.name\`::String, http_method LowCardinality(String) MATERIALIZED attributes.\`http.method\`::String, http_status_code UInt16 MATERIALIZED attributes.\`http.status_code\`::UInt16, has_error Bool MATERIALIZED attributes.\`error\`::Bool, INDEX idx_service service TYPE set(200) GRANULARITY 1, INDEX idx_host host TYPE bloom_filter GRANULARITY 1, INDEX idx_environment environment TYPE set(10) GRANULARITY 1, INDEX idx_k8s_namespace k8s_namespace TYPE set(100) GRANULARITY 1, INDEX idx_http_method http_method TYPE set(20) GRANULARITY 1, INDEX idx_http_status_code http_status_code TYPE minmax GRANULARITY 1, INDEX idx_has_error has_error TYPE set(2) GRANULARITY 1, INDEX idx_fingerprint resource_fingerprint TYPE bloom_filter GRANULARITY 4) ENGINE = MergeTree() PARTITION BY toYYYYMM(timestamp) ORDER BY (team_id, metric_name, service, environment, temporality, timestamp, resource_fingerprint) TTL toDateTime(timestamp) + INTERVAL 30 DAY TO VOLUME 'warm', toDateTime(timestamp) + INTERVAL 90 DAY TO VOLUME 'cold', toDateTime(timestamp) + INTERVAL 365 DAY DELETE SETTINGS index_granularity = 8192, enable_mixed_granularity_parts = 1;"
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
  mariadb:11.4
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

## Python Demo Stack

A self-contained Python demo stack lives in `examples/python-observability-demo/README.md`. It adds sample services that communicate over HTTP, gRPC, and Kafka, use MariaDB, MongoDB, and Redis, emit OpenTelemetry data into this backend, and include load/smoke scripts for backend and UI verification.
