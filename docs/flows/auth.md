# Authentication Flow

Two distinct auth paths: **gRPC API key** (OTLP ingestion) and **HTTP session** (web UI product APIs).

---

## gRPC — API key auth (OTLP ingestion)

Used by every OTLP export call on port `:4317`.

```mermaid
flowchart TD
    START([OTLP gRPC request\nx-api-key metadata header]) --> INTERCEPTOR

    INTERCEPTOR["gRPC UnaryInterceptor\ninternal/auth/\n─\nextract x-api-key from metadata"]
    INTERCEPTOR --> HASH["SHA-256 hash\nkey → hex digest\ncache key: optikk:otlp:team_by_api_key:{hex}"]

    HASH --> REDIS_GET["Redis GET\n5 min TTL cache"]

    REDIS_GET -->|HIT — valid teamID| CTX_OK["inject teamID into ctx\nvia auth.WithTeamID\n─\nrequest proceeds"]
    REDIS_GET -->|"HIT — sentinel '0'"| REJECT["return gRPC Unauthenticated"]
    REDIS_GET -->|MISS| DB_QUERY

    DB_QUERY["MySQL query\nSELECT id FROM teams\nWHERE api_key = ? AND active = 1\nLIMIT 1"]

    DB_QUERY -->|found| CACHE_VALID["Redis SET teamID\nTTL 5 min"] --> CTX_OK
    DB_QUERY -->|not found| CACHE_SENTINEL["Redis SET '0' sentinel\nTTL 5 min"] --> REJECT
```

**Key file:** `internal/auth/resolver.go` — `Authenticator.ResolveTeamID()`

The sentinel `"0"` prevents thundering-herd DB lookups when an invalid key is replayed at ingestion rates (200k+ rps). Invalid keys pay one DB round-trip, then are gate-kept by Redis for 5 minutes.

---

## HTTP — session auth (web UI / product APIs)

Used by every request to `/api/v1/...`.

```mermaid
flowchart TD
    START([HTTP request\nsession cookie]) --> SESSION

    subgraph SESSION_LAYER ["Session layer — internal/infra/session/"]
        SESSION["SessionManager.LoadAndSave\n(scs/redisstore)\n─\nload session from Redis\ncookie → session ID → Redis key"]
        SESSION --> PUBLIC_CHECK["isPublicRequest?\n─\n/api/v1/auth/login\n/otlp/\n/health\n/api/v1/auth/forgot-password\n/api/v1/users  POST\n/api/v1/teams  POST"]
    end

    PUBLIC_CHECK -->|public route| HANDLER_DIRECT["handler — no auth required"]
    PUBLIC_CHECK -->|protected route| GET_STATE

    subgraph TENANT_MW ["TenantMiddleware — internal/infra/middleware/"]
        GET_STATE["GetAuthState\n─\nread from session store:\nauth_user_id\nauth_email\nauth_role\nauth_default_team_id\nauth_team_ids  (comma-sep list)"]

        GET_STATE -->|no user ID| ABORT_401["abort 401 Unauthorized"]
        GET_STATE -->|authenticated| TEAM_RESOLVE

        TEAM_RESOLVE["resolveTeam\n─\n1. read X-Team-Id header\n2. fallback to default_team_id\n3. absent → MISSING_TEAM 403"]

        TEAM_RESOLVE --> AUTHZ["authorizedForTeam?\nteamID ∈ auth_team_ids\nOR == default_team_id"]
        AUTHZ -->|authorised| ATTACH["c.Set tenantKey\nTenantContext{\n  TeamID, UserID,\n  UserEmail, UserRole\n}"]
        AUTHZ -->|forbidden| ABORT_403["abort 403 FORBIDDEN_TEAM"]
    end

    ATTACH --> HANDLER["module handler\nh.GetTenant(c) → TenantContext"]
```

**Key files:**
- `internal/infra/session/manager.go` — `SessionManager`, `GetAuthState`, `CreateAuthSession`, `DestroySession`
- `internal/infra/middleware/middleware.go` — `TenantMiddleware`, `isPublicRequest`, `resolveTeam`

---

## Session lifecycle

```mermaid
flowchart LR
    LOGIN["POST /api/v1/auth/login\n(email + password)"] --> VERIFY["verify credentials\nMySQL users table"]
    VERIFY -->|valid| CREATE["SessionManager.CreateAuthSession\n─\nRenewToken — rotate session ID\nclearAuthState — wipe old keys\nstore auth_{user_id,email,role,\n  default_team_id,team_ids}"]
    CREATE --> COOKIE["Set-Cookie: session=...\nHTTP-only, Secure"]
    COOKIE --> REQUESTS["Subsequent requests\ncarry session cookie\n→ HTTP auth flow above"]
    REQUESTS --> LOGOUT["POST /api/v1/auth/logout"]
    LOGOUT --> DESTROY["SessionManager.DestroySession\n─\nDestroy(ctx) — delete from Redis\nclient cookie expired"]
```

---

## Public route prefixes (no auth required)

| Method | Path prefix |
|--------|-------------|
| ANY | `/api/v1/auth/login` |
| ANY | `/otlp/` |
| ANY | `/health` |
| POST | `/api/v1/auth/forgot-password` |
| POST | `/api/v1/users` |
| POST | `/api/v1/teams` |
