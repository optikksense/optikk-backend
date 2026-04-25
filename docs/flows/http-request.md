# HTTP Request Lifecycle

Covers application bootstrap and the per-request middleware chain for product API calls.

---

## Application bootstrap

```mermaid
flowchart TD
    MAIN["cmd/server/main.go\nmain()\n─\ninitLogger\nconfig.Load\nsignal.NotifyContext SIGINT/SIGTERM"]
    MAIN --> APP_NEW

    subgraph INIT ["internal/app/server/app.go — server.New(cfg)"]
        APP_NEW["appotel.Init\n─\nOTel tracer + logger providers\nW3C propagator\non error: no-op provider, continue"]
        APP_NEW --> INFRA["newInfra(cfg)\n─\nMySQL client\nClickHouse client\nRedis client\nKafka broker client\nSessionManager (scs/redisstore)\ngRPC auth interceptor"]
        INFRA --> MODULES["configuredModules\ninternal/app/server/modules_manifest.go\n─\nconfiguredModules() wires all 49 modules\neach module: handler + routes (+ gRPC if applicable)"]
    end

    MODULES --> START

    subgraph RUN ["app.Start(ctx)"]
        START["startBackgroundModules\n─\ncall Start() on BackgroundRunner modules\n(spans: Assembler.Start + Dispatcher.Start)"]
        START --> RUNGROUP["oklog/run group\n─\nHTTP server actor\ngRPC server actor\nKafka LagPoller actors"]
    end

    RUNGROUP --> SERVE["Serve until ctx cancelled\n(SIGINT / SIGTERM)"]

    SERVE -->|shutdown| DRAIN["stopBackgroundModules\nInfra.Close\notelShutdown (flush traces)"]
```

---

## Gin router setup

```mermaid
flowchart TD
    ROUTER["app.Router()\ninternal/app/server/routes.go\n─\ngin.SetMode ReleaseMode\ngin.New engine"]

    ROUTER --> GLOBAL_MW

    subgraph GLOBAL_MW ["Global middleware (every request)"]
        direction LR
        R1["ErrorRecovery\npanic → 500"]
        R2["otelgin\nOTel trace context\nextract W3C headers\nopen HTTP span"]
        R3["ObservabilityMiddleware\nPrometheus HTTP metrics\naccess log via slog"]
        R4["CORS\nallows traceparent / tracestate"]
        R5["BodyLimit\nreject > 10 MiB"]
        R6["Gzip\ncompress response"]
        R1 --> R2 --> R3 --> R4 --> R5 --> R6
    end

    GLOBAL_MW --> ROUTES

    subgraph ROUTES ["Route groups"]
        HEALTH["GET /health\nGET /health/live  → 200 ok always\nGET /health/ready → probe MySQL / CH / Redis"]
        METRICS_EP["GET /metrics\nPrometheus text format"]
        API_BASE["POST/GET /api/v1/...\nTenantMiddleware\n(session load → auth → team resolve)"]
        API_CACHED["POST/GET /api/v1/...\nTenantMiddleware\n+ Redis response cache\n(logs explorer, some overview)"]
    end
```

---

## Per-request flow (product API)

```mermaid
flowchart TD
    CLIENT([Browser / API client]) -->|HTTP request| GIN["Gin router\nmatch route → module handler"]

    GIN --> MW_CHAIN

    subgraph MW_CHAIN ["Middleware chain (in order)"]
        M1["ErrorRecovery"]
        M2["OTel — extract traceparent\nopen HTTP server span"]
        M3["ObservabilityMiddleware\nstart timer, record method/route"]
        M4["CORS"]
        M5["BodyLimit"]
        M6["Gzip"]
        M7["SessionManager.LoadAndSave\nload session from Redis"]
        M8["TenantMiddleware\nGetAuthState → resolveTeam\nattach TenantContext to Gin ctx"]
        M1 --> M2 --> M3 --> M4 --> M5 --> M6 --> M7 --> M8
    end

    M8 -->|cached route?| CACHE_CHECK
    M8 -->|uncached route| HANDLER

    CACHE_CHECK["Redis response cache\nkey = method + path + team_id + body hash"]
    CACHE_CHECK -->|HIT| RESP_CACHED["return cached JSON\n(skip handler)"]
    CACHE_CHECK -->|MISS| HANDLER

    subgraph MODULE ["Module handler (e.g. traces/explorer)"]
        HANDLER["handler.go\nparse query params + body\ncall service"]
        HANDLER --> SERVICE["service.go\nbusiness logic\ncall repository"]
        SERVICE --> REPO["repository.go\nClickHouse / MySQL query\nvia SelectCH / GetSQL helpers"]
        REPO --> TIER["rollup.TierTableFor\npick coarsest safe rollup tier\n(≤3h→_1m, ≤24h→_5m, >24h→_1h)"]
        TIER --> DB[("ClickHouse / MySQL")]
    end

    DB --> RESP_BUILD["build response DTO\nRespondOK / RespondError\n(internal/shared/httputil/base.go)"]
    RESP_BUILD -->|cached route + miss| CACHE_STORE["store response in Redis"]
    RESP_BUILD --> RESP_CLIENT([JSON response to client])
    CACHE_STORE --> RESP_CLIENT

    RESP_CLIENT --> POST_MW["ObservabilityMiddleware\nrecord status code + duration\nwrite access log\nOTel: end HTTP span"]
```

---

## Middleware order reference

| # | Middleware | File |
|---|-----------|------|
| 1 | ErrorRecovery | `internal/infra/middleware/` |
| 2 | OTel (otelgin) | `internal/infra/otel/` |
| 3 | ObservabilityMiddleware | `internal/infra/middleware/observability.go` |
| 4 | CORS | `internal/infra/middleware/` |
| 5 | BodyLimit | `internal/infra/middleware/` |
| 6 | Gzip | standard Gin middleware |
| 7 | SessionManager.Wrap | `internal/infra/session/manager.go` |
| 8 | TenantMiddleware | `internal/infra/middleware/middleware.go` |
| 9 | Redis response cache | `internal/infra/middleware/` (cached group only) |

---

## Health check logic (`/health/ready`)

Probes all three dependencies with a shared 5-second context timeout:

- **MySQL** — `db.PingContext(ctx)`
- **ClickHouse** — `ch.Ping(ctx)`
- **Redis** — `rdb.Ping(ctx)`

Returns `503 Service Unavailable` if any probe fails; `200 {"status":"ok"}` otherwise.

---

## Key response helpers

| Helper | Purpose | File |
|--------|---------|------|
| `RespondOK(c, data)` | 200 JSON `{success:true, data:…}` | `internal/shared/httputil/base.go` |
| `RespondError(c, code, msg)` | 4xx/5xx JSON `{success:false, error:…}` | same |
| `RespondErrorWithCause(c, code, msg, err)` | same + logs cause | same |
| `ParseRequiredRange(c)` | extract + validate `startMs`/`endMs` | same |
| `ParseInt64Param(c, key)` | path/query int64 extraction | same |
| `ParsePageSize(c, def, max)` | bounded pagination size | same |
