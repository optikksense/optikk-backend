# Observability Backend — Architecture & API Reference

## Table of Contents

1. [Overview](#overview)
2. [Technology Stack](#technology-stack)
3. [Project Structure](#project-structure)
4. [Server Startup & Initialization](#server-startup--initialization)
5. [Request Processing Pipeline](#request-processing-pipeline)
6. [Authentication & Authorization](#authentication--authorization)
7. [Multi-Tenancy](#multi-tenancy)
8. [Database Architecture](#database-architecture)
9. [Telemetry Ingestion Pipeline](#telemetry-ingestion-pipeline)
10. [API Reference](#api-reference)
11. [Module Architecture](#module-architecture)
12. [Error Handling](#error-handling)
13. [Configuration](#configuration)
14. [Deployment](#deployment)

---

## Overview

The Observability Backend is a full-stack observability platform built in **Go** that collects, stores, and queries telemetry data (traces, logs, metrics) using the **OpenTelemetry Protocol (OTLP)**. It provides a REST API for a frontend dashboard to visualize service health, performance, errors, deployments, AI model monitoring, and more.

**Key Capabilities:**

- OTLP-compatible telemetry ingestion (traces, logs, metrics)
- Distributed tracing with span correlation
- Log aggregation and search
- Service topology and dependency mapping
- Health checks and uptime monitoring
- Deployment tracking
- AI/LLM request monitoring (cost, latency, security)
- Alert management
- Multi-tenant data isolation
- Dashboard configuration persistence

---

## Technology Stack

| Component         | Technology                  | Purpose                                   |
| ----------------- | --------------------------- | ----------------------------------------- |
| Language          | Go 1.24                     | Application runtime                       |
| HTTP Framework    | Gin                         | REST API routing and middleware            |
| Relational DB     | MySQL / MariaDB             | Users, teams, alerts, health checks       |
| Analytics DB      | ClickHouse                  | Time-series telemetry (spans, logs, metrics) |
| Message Queue     | Apache Kafka (optional)     | Async telemetry ingestion buffer          |
| Authentication    | JWT (HS256)                 | User session tokens                       |
| Password Hashing  | bcrypt                      | Secure credential storage                 |
| Protocol          | OTLP Protobuf (gzip)       | Telemetry wire format                     |
| Containerization  | Docker (multi-stage alpine) | Deployment packaging                      |

---

## Project Structure

```
observability-backend/
├── cmd/server/
│   └── main.go                          # Application entry point
│
├── internal/                            # Private application packages
│   ├── config/
│   │   └── config.go                    # Environment-based configuration
│   ├── contracts/
│   │   ├── response.go                  # Standard API response envelope
│   │   ├── context.go                   # Tenant context struct
│   │   └── dto.go                       # Common request DTOs
│   ├── database/
│   │   ├── mysql.go                     # MySQL connection pool
│   │   ├── clickhouse.go               # ClickHouse connection pool
│   │   └── query.go                     # Query builder helpers
│   ├── helpers/
│   │   └── handler_helpers.go           # Shared handler utilities
│   ├── modules/                         # Internal feature modules
│   │   ├── alerts/                      # Alert management
│   │   ├── ai/                          # AI/LLM monitoring
│   │   ├── dashboardconfig/             # Dashboard layout persistence
│   │   ├── deployments/                 # Deployment tracking
│   │   ├── explore/                     # Saved queries
│   │   ├── health/                      # Health checks
│   │   ├── infrastructure/              # Infrastructure nodes
│   │   ├── insights/                    # Derived insights (SLO, utilization)
│   │   └── saturation/                  # Resource saturation
│   └── platform/
│       ├── auth/
│       │   └── auth.go                  # JWT token manager
│       ├── middleware/
│       │   └── middleware.go            # Global middleware stack
│       └── server/
│           ├── app.go                   # Application bootstrap & module init
│           └── router.go               # Route registration
│
├── modules/                             # External feature modules
│   ├── user/                            # User, team, auth management
│   ├── metrics/                         # Metrics querying
│   ├── logs/                            # Log querying
│   ├── spans/                           # Trace & span querying
│   └── ingestion/                       # OTLP telemetry ingestion
│       ├── api/
│       │   ├── handler.go               # OTLP HTTP endpoints
│       │   ├── auth.go                  # API key validation
│       │   └── translate/               # OTLP → internal model translators
│       └── ingest/
│           └── impl/
│               ├── direct.go            # Direct ClickHouse insertion
│               ├── kafka_producer.go    # Kafka async producer
│               └── kafka_consumer.go    # Kafka batch consumer
│
├── db/
│   ├── schema.sql                       # MySQL schema
│   └── clickhouse_schema.sql            # ClickHouse schema
│
├── docs/                                # Documentation
├── scripts/                             # Utility scripts
├── Dockerfile                           # Multi-stage Docker build
├── go.mod / go.sum                      # Go module dependencies
└── go.work                              # Go workspace (multi-module)
```

---

## Server Startup & Initialization

**Entry point:** `cmd/server/main.go`

The server follows a sequential initialization process:

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Load Configuration                                           │
│    config.Load() reads environment variables with defaults       │
│                                                                 │
│ 2. Connect to MySQL                                             │
│    database.Open() creates connection pool (max 20 conns)       │
│                                                                 │
│ 3. Connect to ClickHouse                                        │
│    database.OpenClickHouse() with native protocol               │
│                                                                 │
│ 4. Create Application                                           │
│    server.New(dbConn, chConn, cfg) initializes all modules      │
│                                                                 │
│ 5. Start HTTP Server                                            │
│    Listens on configured PORT (default 8080)                    │
│    Wrapped with h2c for HTTP/2 cleartext support                │
│                                                                 │
│ 6. Graceful Shutdown                                            │
│    On SIGTERM/SIGINT: 10s drain → close Kafka → close DBs       │
└─────────────────────────────────────────────────────────────────┘
```

**Module Initialization (inside `server.New`):**

Each module is instantiated with its dependencies (DB connections, config). The `app.go` file:

1. Creates the Gin engine with middleware
2. Instantiates each module's handler (passing DB connections)
3. If Kafka is enabled, starts the Kafka producer and consumer goroutines
4. Registers all routes via `router.go`

---

## Request Processing Pipeline

Every HTTP request goes through the following pipeline:

```
                        ┌──────────────────┐
                        │  HTTP Request In  │
                        └────────┬─────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Gin Router Matching    │
                    │  (path + method → handler)│
                    └────────────┬────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │        GLOBAL MIDDLEWARE CHAIN       │
              │                                      │
              │  1. Logger                           │
              │     → Logs method, path, status      │
              │                                      │
              │  2. ErrorRecovery                    │
              │     → Catches panics → 500 JSON      │
              │                                      │
              │  3. CORS Middleware                   │
              │     → Adds Access-Control-* headers   │
              │     → Handles OPTIONS preflight       │
              │                                      │
              │  4. TenantMiddleware                  │
              │     → Extracts JWT or X-* headers     │
              │     → Sets TenantContext in ctx       │
              └──────────────────┬──────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │    Route Handler         │
                    │                          │
                    │  • Parse params/body     │
                    │  • Get TenantContext     │
                    │  • Call service layer    │
                    │  • Return response       │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   JSON Response Out      │
                    │                          │
                    │  { success, data, error, │
                    │    pagination, timestamp }│
                    └─────────────────────────┘
```

### Standard Response Envelope

All API responses use a consistent structure:

```json
{
  "success": true,
  "data": { ... },
  "pagination": {
    "page": 1,
    "pageSize": 20,
    "total": 150,
    "totalPages": 8
  },
  "timestamp": "2025-01-15T10:30:00Z"
}
```

Error responses:

```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid email format",
    "timestamp": "2025-01-15T10:30:00Z",
    "path": "/api/signup",
    "fieldErrors": {
      "email": "must be a valid email address"
    }
  },
  "timestamp": "2025-01-15T10:30:00Z"
}
```

### Common Handler Helpers

| Helper               | Purpose                                                 |
| -------------------- | ------------------------------------------------------- |
| `ParseRange(c)`      | Extracts `startTime`/`endTime` query params for time-series |
| `ParseInt64Param()`  | Parses and validates int64 query parameters             |
| `ParseListParam()`   | Parses comma-separated or repeated query param values   |
| `ExtractIDParam()`   | Extracts `:id` path parameter                           |
| `RespondOK()`        | Wraps data in success response envelope                 |
| `RespondError()`     | Wraps error in error response envelope                  |
| `GetTenant(c)`       | Retrieves TenantContext from Gin context                |

---

## Authentication & Authorization

The system uses **two separate authentication mechanisms** depending on the type of request:

### 1. JWT Authentication (Dashboard/API Users)

Used for all `/api/*` routes (dashboard, management, querying).

```
┌──────────────────────────────────────────────────────┐
│                  JWT Auth Flow                        │
│                                                      │
│  POST /api/auth/login                                │
│    ├── Validate email + bcrypt password               │
│    ├── Generate JWT (HS256)                           │
│    │   Claims: userId, email, name, role,             │
│    │           organizationId, teamId                 │
│    │   Expiry: configurable (default 24h)             │
│    └── Return token in response body                  │
│                                                      │
│  Subsequent requests:                                │
│    Authorization: Bearer <jwt_token>                  │
│    → TenantMiddleware decodes claims                  │
│    → Sets TenantContext for handler                   │
│                                                      │
│  Fallback (no JWT):                                  │
│    X-Organization-Id, X-Team-Id, X-User-Id headers   │
│    → Used for service-to-service calls                │
│    → Defaults to OrgID=1, TeamID=1 if missing         │
└──────────────────────────────────────────────────────┘
```

### 2. API Key Authentication (Telemetry Ingestion)

Used for all `/otlp/*` routes (telemetry ingestion from instrumented services).

```
┌──────────────────────────────────────────────────────┐
│               API Key Auth Flow                       │
│                                                      │
│  POST /otlp/v1/{traces|logs|metrics}                 │
│    Headers (either):                                 │
│      Authorization: Bearer <api_key>                  │
│      X-API-Key: <api_key>                             │
│                                                      │
│  Validation:                                         │
│    → Look up api_key in teams.api_key column          │
│    → Map to team_id for data isolation                │
│    → Reject if key is invalid                         │
└──────────────────────────────────────────────────────┘
```

---

## Multi-Tenancy

Data is isolated by **organization** and **team**:

```go
type TenantContext struct {
    OrganizationID int64   // Top-level organizational boundary
    TeamID         int64   // Team within the organization
    UserID         int64   // Authenticated user
    UserEmail      string
    UserRole       string  // "admin" | "member"
}
```

**Isolation Rules:**

- Every MySQL table includes `organization_id` and/or `team_id` columns
- Every ClickHouse table includes `team_id` as the first column in the sort key
- All queries filter by the tenant context extracted from the request
- API keys map to a specific team, ensuring ingested telemetry is scoped
- No cross-tenant data leakage is possible at the query layer

---

## Database Architecture

### Dual-Database Strategy

The system uses **two databases** optimized for different workloads:

```
┌─────────────────────────────┐    ┌─────────────────────────────┐
│         MySQL/MariaDB        │    │         ClickHouse           │
│                              │    │                              │
│  Purpose: Relational data    │    │  Purpose: Time-series data   │
│  ACID transactions           │    │  Columnar storage            │
│  Low-volume, high-consistency│    │  High-volume, fast analytics │
│                              │    │                              │
│  Tables:                     │    │  Tables:                     │
│  • users                     │    │  • spans (TTL: 30 days)      │
│  • teams                     │    │  • logs (TTL: 30 days)       │
│  • user_teams                │    │  • metrics (TTL: 30 days)    │
│  • alerts                    │    │  • deployments               │
│  • health_checks             │    │  • health_check_results      │
│  • explore_saved_queries     │    │    (TTL: 7 days)             │
│  • dashboard_chart_configs   │    │  • ai_requests               │
└─────────────────────────────┘    └─────────────────────────────┘
```

### MySQL Schema

**users** — User accounts with bcrypt-hashed passwords.

| Column          | Type         | Notes                        |
| --------------- | ------------ | ---------------------------- |
| id              | BIGINT PK    | Auto-increment               |
| organization_id | BIGINT       | Tenant isolation             |
| email           | VARCHAR(255) | UNIQUE, login identifier     |
| password_hash   | VARCHAR(255) | bcrypt hash                  |
| name            | VARCHAR(255) | Display name                 |
| role            | VARCHAR(50)  | "admin" or "member"          |
| active          | BOOLEAN      | Account enabled              |
| last_login_at   | TIMESTAMP    | Last successful login        |

**teams** — Logical groupings with API keys for telemetry.

| Column          | Type          | Notes                       |
| --------------- | ------------- | --------------------------- |
| id              | BIGINT PK     | Auto-increment              |
| organization_id | BIGINT        | Tenant isolation            |
| name            | VARCHAR(255)  | Team display name           |
| slug            | VARCHAR(100)  | URL-friendly identifier     |
| api_key         | VARCHAR(255)  | UNIQUE, for OTLP ingestion  |

**alerts** — Alert definitions and lifecycle state.

| Column           | Type          | Notes                                |
| ---------------- | ------------- | ------------------------------------ |
| id               | BIGINT PK     | Auto-increment                       |
| name             | VARCHAR(255)  | Alert rule name                      |
| type             | VARCHAR(50)   | Alert type                           |
| severity         | VARCHAR(20)   | critical, warning, info              |
| status           | VARCHAR(20)   | firing, acknowledged, resolved, muted|
| condition_expr   | TEXT          | Condition expression                 |
| metric           | VARCHAR(255)  | Metric being monitored               |
| operator         | VARCHAR(10)   | >, <, >=, <=, ==                     |
| threshold        | DOUBLE        | Threshold value                      |
| current_value    | DOUBLE        | Value that triggered alert           |

**health_checks** — Uptime monitoring configuration.

| Column           | Type          | Notes                       |
| ---------------- | ------------- | --------------------------- |
| id               | BIGINT PK     | Auto-increment              |
| name             | VARCHAR(255)  | Check display name          |
| type             | VARCHAR(50)   | http, tcp, dns, etc.        |
| target_url       | TEXT          | URL to monitor              |
| interval_seconds | INT           | Check frequency             |
| timeout_ms       | INT           | Request timeout             |
| expected_status  | INT           | Expected HTTP status code   |
| enabled          | BOOLEAN       | Active or paused            |

### ClickHouse Schema

**spans** — Distributed trace spans.

| Column         | Type             | Notes                               |
| -------------- | ---------------- | ----------------------------------- |
| team_id        | UInt64           | Tenant isolation                    |
| trace_id       | String           | Links spans into a trace            |
| span_id        | String           | Unique span identifier              |
| parent_span_id | String           | Parent span (empty for root)        |
| is_root        | UInt8            | 1 if root span                      |
| operation_name | String           | e.g., "GET /api/users"              |
| service_name   | LowCardinality   | Originating service                 |
| span_kind      | LowCardinality   | server, client, internal, etc.      |
| start_time     | DateTime64(9)    | Nanosecond precision                |
| duration_ms    | Float64          | Span duration in milliseconds       |
| status         | LowCardinality   | ok, error, unset                    |
| http_method    | LowCardinality   | GET, POST, etc.                     |
| http_status_code | UInt16         | HTTP response code                  |
| attributes     | String (JSON)    | Arbitrary key-value metadata        |

- **Engine:** MergeTree, partitioned monthly, ordered by (team_id, start_time, trace_id, span_id)
- **TTL:** 30 days auto-expiry

**logs** — Application log events.

| Column       | Type             | Notes                               |
| ------------ | ---------------- | ----------------------------------- |
| team_id      | UInt64           | Tenant isolation                    |
| timestamp    | DateTime64(9)    | Nanosecond precision                |
| level        | LowCardinality   | info, warn, error, debug, fatal     |
| service_name | LowCardinality   | Originating service                 |
| message      | String           | Log message body                    |
| trace_id     | String           | Correlation with traces             |
| span_id      | String           | Correlation with spans              |
| exception    | String           | Stack trace (if error)              |
| attributes   | String (JSON)    | Arbitrary metadata                  |

- **Engine:** MergeTree, partitioned monthly, ordered by (team_id, timestamp, level, service_name)
- **TTL:** 30 days

**metrics** — Aggregated metric measurements.

| Column        | Type             | Notes                              |
| ------------- | ---------------- | ---------------------------------- |
| team_id       | UInt64           | Tenant isolation                   |
| metric_name   | String           | e.g., "http.request.duration"      |
| metric_type   | LowCardinality   | gauge, counter, histogram, summary |
| service_name  | LowCardinality   | Originating service                |
| timestamp     | DateTime64(9)    | Measurement time                   |
| value         | Float64          | Scalar value (gauge/counter)       |
| p50/p95/p99   | Float64          | Percentile values (histograms)     |
| attributes    | String (JSON)    | Arbitrary dimensions               |

- **Engine:** MergeTree, partitioned monthly, ordered by (team_id, timestamp, metric_type, service_name)
- **TTL:** 30 days

**ai_requests** — AI/LLM call monitoring.

| Column            | Type    | Notes                              |
| ----------------- | ------- | ---------------------------------- |
| model_name        | String  | e.g., "gpt-4", "claude-3"         |
| model_provider    | String  | e.g., "openai", "anthropic"       |
| duration_ms       | Float64 | Response latency                   |
| cost_usd          | Float64 | Estimated cost                     |
| tokens_prompt     | UInt64  | Input token count                  |
| tokens_completion | UInt64  | Output token count                 |
| pii_detected      | UInt8   | Whether PII was found              |
| guardrail_blocked | UInt8   | Whether content policy triggered   |

---

## Telemetry Ingestion Pipeline

The ingestion pipeline converts OpenTelemetry data into ClickHouse rows. It supports **two modes:**

### Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                     Instrumented Services                         │
│    (using OpenTelemetry SDKs / OTLP Exporters)                   │
└─────────────┬─────────────────┬──────────────────┬───────────────┘
              │ POST            │ POST             │ POST
              │ /otlp/v1/traces │ /otlp/v1/logs    │ /otlp/v1/metrics
              ▼                 ▼                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                    OTLP Ingestion Handler                        │
│                                                                  │
│  1. API Key Authentication                                       │
│     → Validate Authorization header or X-API-Key                 │
│     → Map api_key → team_id                                      │
│                                                                  │
│  2. Decode Protobuf                                              │
│     → Decompress gzip                                            │
│     → Unmarshal OTLP ExportRequest                               │
│                                                                  │
│  3. Translate                                                    │
│     → OTLP ResourceSpans → internal Span model                  │
│     → OTLP ResourceLogs  → internal Log model                   │
│     → OTLP ResourceMetrics → internal Metric model              │
│     → Flattens resource/scope attributes into each record        │
│     → Extracts semantic conventions (http.method, service.name)  │
│                                                                  │
│  4. Ingest                                                       │
│     → Route to configured ingester (Direct or Kafka)             │
└───────────┬───────────────────────────────────────┬──────────────┘
            │                                       │
    ┌───────▼───────┐                     ┌────────▼────────┐
    │  Direct Mode   │                     │   Kafka Mode     │
    │ (dev/testing)  │                     │  (production)    │
    │                │                     │                  │
    │ Synchronous    │                     │ Async publish to │
    │ INSERT into    │                     │ Kafka topics:    │
    │ ClickHouse     │                     │ • otlp-spans     │
    │                │                     │ • otlp-metrics   │
    └───────┬───────┘                     │ • otlp-logs      │
            │                              └────────┬────────┘
            │                                       │
            │                              ┌────────▼────────┐
            │                              │ Kafka Consumer   │
            │                              │                  │
            │                              │ Batch processing │
            │                              │ Size: 500 msgs   │
            │                              │ Flush: 2000ms    │
            │                              │                  │
            │                              │ Batch INSERT     │
            │                              │ into ClickHouse  │
            │                              └────────┬────────┘
            │                                       │
            ▼                                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                        ClickHouse                                │
│          (spans / logs / metrics tables)                          │
└──────────────────────────────────────────────────────────────────┘
```

### Direct Mode (`KAFKA_ENABLED=false`)

- Telemetry is inserted synchronously into ClickHouse within the HTTP request lifecycle
- Simpler setup, lower latency for small volumes
- Suitable for development and testing

### Kafka Mode (`KAFKA_ENABLED=true`)

- Telemetry is serialized to JSON and published to Kafka topics
- A background consumer goroutine reads from Kafka in batches
- Batches are flushed to ClickHouse when either the batch size (500) or time interval (2000ms) is reached
- Provides back-pressure handling, fault tolerance, and decoupled write path
- Uses Sarama SyncProducer with WaitForAll acknowledgment for durability

### Translation Layer

The translators in `modules/ingestion/api/translate/` convert OTLP protobuf structures:

- **Traces:** `ResourceSpans → []Span` — Flattens resource attributes, extracts HTTP semantics, computes `is_root`, calculates `duration_ms`
- **Logs:** `ResourceLogs → []Log` — Extracts severity, maps body to message, preserves trace context
- **Metrics:** `ResourceMetrics → []Metric` — Handles gauge, sum, histogram, summary data points with all percentiles

---

## API Reference

### Identity & Authentication

| Method | Endpoint                            | Purpose                            |
| ------ | ----------------------------------- | ---------------------------------- |
| POST   | `/api/signup`                       | Create a new user account          |
| POST   | `/api/auth/login`                   | Authenticate and receive JWT token |
| POST   | `/api/auth/logout`                  | Logout (client-side token discard) |
| GET    | `/api/auth/me`                      | Get authenticated user profile     |
| GET    | `/api/auth/context`                 | Get auth context (alias for /me)   |
| GET    | `/api/auth/validate`                | Validate current JWT token         |

### User Management

| Method | Endpoint                            | Purpose                            |
| ------ | ----------------------------------- | ---------------------------------- |
| GET    | `/api/users`                        | List all users in organization     |
| GET    | `/api/users/me`                     | Get current user                   |
| GET    | `/api/users/:id`                    | Get user by ID                     |
| POST   | `/api/users`                        | Create user (admin only)           |
| POST   | `/api/users/:userId/teams/:teamId`  | Add user to team                   |
| DELETE | `/api/users/:userId/teams/:teamId`  | Remove user from team              |

### Team Management

| Method | Endpoint                            | Purpose                            |
| ------ | ----------------------------------- | ---------------------------------- |
| GET    | `/api/teams`                        | List all teams                     |
| GET    | `/api/teams/my-teams`               | Get current user's teams           |
| GET    | `/api/teams/:id`                    | Get team by ID                     |
| GET    | `/api/teams/slug/:slug`             | Get team by URL slug               |
| POST   | `/api/teams`                        | Create a new team                  |

### Settings

| Method | Endpoint                            | Purpose                            |
| ------ | ----------------------------------- | ---------------------------------- |
| GET    | `/api/settings/profile`             | Get user profile settings          |
| PUT    | `/api/settings/profile`             | Update user profile                |

### Dashboard & Service Overview

| Method | Endpoint                                      | Purpose                                     |
| ------ | --------------------------------------------- | ------------------------------------------- |
| GET    | `/api/dashboard/overview`                     | High-level dashboard statistics              |
| GET    | `/api/dashboard/services`                     | List all discovered services                 |
| GET    | `/api/dashboard/services/:serviceName`        | Detailed stats for a specific service        |
| GET    | `/api/v1/status`                              | System health status                         |

### Service Metrics

| Method | Endpoint                                             | Purpose                                         |
| ------ | ---------------------------------------------------- | ----------------------------------------------- |
| GET    | `/api/v1/services/topology`                          | Service dependency graph (who calls whom)        |
| GET    | `/api/v1/services/metrics`                           | Aggregated metrics per service (rate, errors, duration) |
| GET    | `/api/v1/services/timeseries`                        | Service metrics over time                        |
| GET    | `/api/v1/services/:serviceName/endpoints`            | Endpoint breakdown for a service                 |
| GET    | `/api/v1/services/dependencies`                      | Service-to-service dependency map                |
| GET    | `/api/v1/services/:serviceName/errors`               | Errors for a specific service                    |
| GET    | `/api/v1/endpoints/metrics`                          | Aggregated metrics per endpoint                  |
| GET    | `/api/v1/endpoints/timeseries`                       | Endpoint metrics over time                       |
| GET    | `/api/v1/metrics/timeseries`                         | Custom metric timeseries query                   |
| GET    | `/api/v1/metrics/summary`                            | Metrics summary / overview                       |

**Common Query Parameters for metrics/timeseries endpoints:**

| Parameter     | Type     | Description                           |
| ------------- | -------- | ------------------------------------- |
| `startTime`   | int64    | Unix timestamp (ms) — range start     |
| `endTime`     | int64    | Unix timestamp (ms) — range end       |
| `service`     | string   | Filter by service name                |
| `operation`   | string   | Filter by operation/endpoint          |
| `interval`    | string   | Aggregation interval (e.g., "1m", "5m") |

### Logs

| Method | Endpoint                            | Purpose                                         |
| ------ | ----------------------------------- | ----------------------------------------------- |
| GET    | `/api/v1/logs`                      | Query logs with filters (service, level, search) |
| GET    | `/api/v1/logs/histogram`            | Log count by level over time buckets             |
| GET    | `/api/v1/logs/volume`               | Total log volume timeseries                      |
| GET    | `/api/v1/logs/stats`                | Log statistics (counts by level, top services)   |
| GET    | `/api/v1/logs/fields`               | Available field names for a service's logs       |
| GET    | `/api/v1/logs/surrounding`          | Context logs around a specific log entry         |
| GET    | `/api/v1/logs/detail`               | Full detail of a single log entry                |
| GET    | `/api/v1/traces/:traceId/logs`      | All logs associated with a trace                 |

**Common Query Parameters for log endpoints:**

| Parameter      | Type     | Description                       |
| -------------- | -------- | --------------------------------- |
| `startTime`    | int64    | Unix timestamp (ms) — range start |
| `endTime`      | int64    | Unix timestamp (ms) — range end   |
| `service`      | string   | Filter by service name            |
| `level`        | string   | Filter by log level               |
| `search`       | string   | Full-text search in message       |
| `traceId`      | string   | Filter by trace ID                |
| `page`         | int      | Pagination page                   |
| `pageSize`     | int      | Results per page                  |

### Traces & Spans

| Method | Endpoint                            | Purpose                                         |
| ------ | ----------------------------------- | ----------------------------------------------- |
| GET    | `/api/v1/traces`                    | Query traces with filters                        |
| GET    | `/api/v1/traces/:traceId/spans`     | Get all spans in a trace (waterfall view)        |
| GET    | `/api/v1/latency/histogram`         | Latency distribution (P50, P95, P99 buckets)     |
| GET    | `/api/v1/latency/heatmap`           | 2D latency heatmap (time × latency bucket)       |
| GET    | `/api/v1/errors/groups`             | Error grouping (by message/type)                 |
| GET    | `/api/v1/errors/timeseries`         | Error rate over time                             |

### Alerts

| Method | Endpoint                                     | Purpose                             |
| ------ | -------------------------------------------- | ----------------------------------- |
| GET    | `/api/alerts`                                | List all alerts                     |
| GET    | `/api/alerts/paged`                          | Paginated alert listing             |
| GET    | `/api/alerts/:id`                            | Get alert detail                    |
| POST   | `/api/alerts`                                | Create a new alert rule             |
| POST   | `/api/alerts/:id/acknowledge`                | Acknowledge a firing alert          |
| POST   | `/api/alerts/:id/resolve`                    | Mark alert as resolved              |
| POST   | `/api/alerts/:id/mute`                       | Mute alert notifications            |
| POST   | `/api/alerts/:id/mute-with-reason`           | Mute with documented reason         |
| POST   | `/api/alerts/bulk/mute`                      | Bulk mute multiple alerts           |
| POST   | `/api/alerts/bulk/resolve`                   | Bulk resolve multiple alerts        |
| GET    | `/api/alerts/for-incident/:policyId`         | Alerts related to an incident       |
| GET    | `/api/alerts/count/active`                   | Count of currently active alerts    |
| GET    | `/api/v1/incidents`                          | List incidents                      |

### Health Checks

| Method | Endpoint                                       | Purpose                           |
| ------ | ---------------------------------------------- | --------------------------------- |
| GET    | `/api/v1/health-checks`                        | List all health checks            |
| POST   | `/api/v1/health-checks`                        | Create a health check             |
| PUT    | `/api/v1/health-checks/:id`                    | Update a health check             |
| DELETE | `/api/v1/health-checks/:id`                    | Delete a health check             |
| PATCH  | `/api/v1/health-checks/:id/toggle`             | Enable/disable a health check     |
| GET    | `/api/v1/health-checks/status`                 | Overall health status summary      |
| GET    | `/api/v1/health-checks/:checkId/results`       | Historical check results           |
| GET    | `/api/v1/health-checks/:checkId/trend`         | Uptime trend data                  |

### Deployments

| Method | Endpoint                                | Purpose                              |
| ------ | --------------------------------------- | ------------------------------------ |
| GET    | `/api/v1/deployments`                   | List deployments                     |
| GET    | `/api/v1/deployments/events`            | Deployment events timeline           |
| GET    | `/api/v1/deployments/:deployId/diff`    | Config/version diff for a deployment |
| POST   | `/api/v1/deployments`                   | Record a new deployment              |

### Infrastructure

| Method | Endpoint                                            | Purpose                                |
| ------ | --------------------------------------------------- | -------------------------------------- |
| GET    | `/api/v1/infrastructure`                            | Infrastructure overview                |
| GET    | `/api/v1/infrastructure/nodes`                      | List infrastructure nodes (hosts)      |
| GET    | `/api/v1/infrastructure/nodes/:host/services`       | Services running on a specific host    |

### Saturation

| Method | Endpoint                              | Purpose                                |
| ------ | ------------------------------------- | -------------------------------------- |
| GET    | `/api/v1/saturation/metrics`          | Resource saturation metrics            |
| GET    | `/api/v1/saturation/timeseries`       | Saturation trends over time            |

### Insights

| Method | Endpoint                                       | Purpose                                       |
| ------ | ---------------------------------------------- | --------------------------------------------- |
| GET    | `/api/v1/insights/resource-utilization`        | CPU, memory, disk utilization insights         |
| GET    | `/api/v1/insights/slo-sli`                     | SLO/SLI compliance and error budget            |
| GET    | `/api/v1/insights/logs-stream`                 | Log stream health insights                     |
| GET    | `/api/v1/insights/database-cache`              | Database and cache performance insights        |
| GET    | `/api/v1/insights/messaging-queue`             | Message queue health and backlog insights      |

### AI / LLM Monitoring

| Method | Endpoint                                          | Purpose                                     |
| ------ | ------------------------------------------------- | ------------------------------------------- |
| GET    | `/api/v1/ai/summary`                              | AI usage summary                            |
| GET    | `/api/v1/ai/models`                               | List tracked AI models                      |
| GET    | `/api/v1/ai/performance/metrics`                  | Model performance metrics                   |
| GET    | `/api/v1/ai/performance/timeseries`               | Performance trends over time                |
| GET    | `/api/v1/ai/performance/latency-histogram`        | Model response latency distribution         |
| GET    | `/api/v1/ai/cost/metrics`                         | Cost breakdown by model/provider            |
| GET    | `/api/v1/ai/cost/timeseries`                      | Cost trends over time                       |
| GET    | `/api/v1/ai/cost/token-breakdown`                 | Token usage breakdown (prompt/completion)    |
| GET    | `/api/v1/ai/security/metrics`                     | Security metrics (PII, guardrails)          |
| GET    | `/api/v1/ai/security/timeseries`                  | Security events over time                   |
| GET    | `/api/v1/ai/security/pii-categories`              | PII category breakdown                      |

### Dashboard Configuration

| Method | Endpoint                                | Purpose                                  |
| ------ | --------------------------------------- | ---------------------------------------- |
| GET    | `/api/v1/dashboard-config/pages`        | List dashboard pages                     |
| GET    | `/api/v1/dashboard-config/:pageId`      | Get dashboard layout for a page          |
| PUT    | `/api/v1/dashboard-config/:pageId`      | Save/update dashboard layout             |

### Explore / Saved Queries

| Method | Endpoint                                    | Purpose                            |
| ------ | ------------------------------------------- | ---------------------------------- |
| GET    | `/api/v1/explore/saved-queries`             | List saved queries                 |
| POST   | `/api/v1/explore/saved-queries`             | Create a saved query               |
| PUT    | `/api/v1/explore/saved-queries/:id`         | Update a saved query               |
| DELETE | `/api/v1/explore/saved-queries/:id`         | Delete a saved query               |

### OTLP Ingestion

| Method | Endpoint                | Purpose                                 |
| ------ | ----------------------- | --------------------------------------- |
| POST   | `/otlp/v1/traces`      | Ingest trace spans (OTLP protobuf)      |
| POST   | `/otlp/v1/logs`        | Ingest log records (OTLP protobuf)      |
| POST   | `/otlp/v1/metrics`     | Ingest metric data points (OTLP protobuf) |

---

## Module Architecture

Each feature module follows a **Clean Architecture** pattern with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────┐
│                     HTTP Layer                           │
│                                                         │
│  handler.go / module.go                                 │
│  • Route registration                                   │
│  • Request parsing & validation                         │
│  • Response formatting                                  │
│  • Delegates to service layer                           │
└──────────────────────┬──────────────────────────────────┘
                       │ calls
┌──────────────────────▼──────────────────────────────────┐
│                   Service Layer                          │
│                                                         │
│  service/interfaces/  — Contracts (interfaces)           │
│  service/impl/        — Business logic implementation    │
│  • Validation rules                                     │
│  • Data transformation                                  │
│  • Orchestration between stores                         │
└──────────────────────┬──────────────────────────────────┘
                       │ calls
┌──────────────────────▼──────────────────────────────────┐
│                    Store Layer                            │
│                                                         │
│  store/interfaces/  — Repository contracts               │
│  store/impl/        — SQL queries & data access          │
│  • MySQL queries (relational data)                       │
│  • ClickHouse queries (telemetry data)                   │
│  • Raw SQL with parameterized queries                    │
└──────────────────────┬──────────────────────────────────┘
                       │
              ┌────────▼────────┐
              │   Database(s)    │
              └─────────────────┘
```

### Module List & Responsibilities

| Module               | Database    | Responsibility                                           |
| -------------------- | ----------- | -------------------------------------------------------- |
| **user**             | MySQL       | Authentication, user CRUD, team management, profiles     |
| **metrics**          | ClickHouse  | Service/endpoint metrics, timeseries, topology           |
| **logs**             | ClickHouse  | Log search, histograms, volume, field discovery          |
| **spans**            | ClickHouse  | Trace queries, span waterfalls, latency, error analysis  |
| **ingestion**        | ClickHouse  | OTLP receive, translate, and persist telemetry           |
| **alerts**           | MySQL       | Alert lifecycle (create, acknowledge, resolve, mute)     |
| **health**           | MySQL + CH  | Health check config (MySQL) + results (ClickHouse)       |
| **deployments**      | ClickHouse  | Deployment tracking and diffing                          |
| **infrastructure**   | ClickHouse  | Host/node discovery from telemetry attributes            |
| **ai**               | ClickHouse  | LLM request monitoring, cost, security, performance      |
| **saturation**       | ClickHouse  | Resource saturation analysis from metrics                |
| **insights**         | ClickHouse  | Derived insights (SLO, utilization, queue health)        |
| **dashboardconfig**  | MySQL       | User dashboard layout persistence                        |
| **explore**          | MySQL       | Saved query management                                   |

---

## Error Handling

### Service Error Types

The user module defines typed service errors that map to HTTP status codes:

| Error Type                | HTTP Status | When Used                              |
| ------------------------- | ----------- | -------------------------------------- |
| `ServiceErrorValidation`  | 400         | Invalid input, missing fields          |
| `ServiceErrorUnauthorized`| 401         | Bad credentials, expired token         |
| `ServiceErrorNotFound`    | 404         | Resource not found                     |
| `ServiceErrorInternal`    | 500         | Unexpected server-side failures        |

### Global Panic Recovery

The `ErrorRecovery` middleware catches any unhandled panics in handlers and returns a structured 500 response, preventing the server from crashing.

### Password Security

- Passwords are hashed with **bcrypt** (standard cost factor)
- Plaintext passwords are never logged or stored
- Comparison uses constant-time bcrypt.CompareHashAndPassword

---

## Configuration

All configuration is via **environment variables** with sensible defaults:

| Variable               | Default         | Description                              |
| ---------------------- | --------------- | ---------------------------------------- |
| `PORT`                 | `8080`          | HTTP server port                         |
| `MYSQL_HOST`           | `127.0.0.1`    | MySQL/MariaDB host                       |
| `MYSQL_PORT`           | `3306`          | MySQL port                               |
| `MYSQL_DATABASE`       | `observability` | MySQL database name                      |
| `MYSQL_USERNAME`       | `root`          | MySQL username                           |
| `MYSQL_PASSWORD`       | `root123`       | MySQL password                           |
| `CLICKHOUSE_HOST`      | `127.0.0.1`    | ClickHouse host                          |
| `CLICKHOUSE_PORT`      | `9000`          | ClickHouse native protocol port          |
| `CLICKHOUSE_DATABASE`  | `observability` | ClickHouse database name                 |
| `CLICKHOUSE_USERNAME`  | `default`       | ClickHouse username                      |
| `CLICKHOUSE_PASSWORD`  | `clickhouse123` | ClickHouse password                      |
| `JWT_SECRET`           | (built-in)      | JWT signing secret (**must override**)   |
| `JWT_EXPIRATION_MS`    | `86400000`      | Token expiration (default: 24 hours)     |
| `KAFKA_ENABLED`        | `true`          | Enable Kafka-based ingestion             |
| `KAFKA_BROKERS`        | `localhost:9092` | Comma-separated Kafka broker addresses  |
| `QUEUE_BATCH_SIZE`     | `500`           | Kafka consumer batch size                |
| `QUEUE_FLUSH_INTERVAL_MS` | `2000`       | Kafka consumer flush interval (ms)       |

### Database Connection Pooling

- **Max lifetime:** 3 minutes
- **Max open connections:** 20
- **Max idle connections:** 10

---

## Deployment

### Docker Build

The project uses a **multi-stage Docker build** for minimal image size:

```dockerfile
# Stage 1: Build
FROM golang:1.24-alpine AS builder
# Download deps, compile binary with stripped symbols

# Stage 2: Runtime
FROM alpine:3.20
# Copy binary + DB schemas
EXPOSE 8080
CMD ["./observability-go"]
```

### Production Checklist

1. **Override `JWT_SECRET`** — The default is insecure for production
2. **Set `KAFKA_ENABLED=true`** — Use Kafka for production-grade ingestion
3. **Run MySQL migrations** — Apply `db/schema.sql` before first start
4. **Run ClickHouse migrations** — Apply `db/clickhouse_schema.sql`
5. **Configure connection pooling** — Tune based on expected load
6. **Set up Kafka topics** — `otlp-spans`, `otlp-metrics`, `otlp-logs`
7. **Monitor ClickHouse TTL** — Data auto-expires (30 days for telemetry, 7 days for health results)

### Graceful Shutdown Sequence

1. Receive SIGTERM/SIGINT
2. Stop accepting new HTTP connections
3. Wait up to 10 seconds for in-flight requests to complete
4. Close Kafka producer and consumer
5. Close database connections
6. Exit
