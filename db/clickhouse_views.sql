-- ---------------------------------------------------------------------------
-- Materialized views for pre-aggregated query acceleration.
--
-- These views maintain running aggregates that update automatically as data
-- is inserted into spans and logs. Dashboard and metrics queries should read
-- from these views instead of scanning raw tables, reducing query latency
-- from seconds to milliseconds at petabyte scale.
--
-- Apply AFTER clickhouse_schema.sql.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- spans_service_1m
-- Per-service per-minute request/error counts and latency percentiles.
-- Feeds: dashboard overview, service health, service time series.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_service_1m
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (team_id, service_name, minute)
TTL minute + INTERVAL 30 DAY
AS
SELECT
    team_id,
    service_name,
    toStartOfMinute(start_time) AS minute,
    countState()                               AS request_count,
    countIfState(status = 'ERROR' OR http_status_code >= 400) AS error_count,
    quantileState(0.50)(duration_ms)           AS p50_state,
    quantileState(0.95)(duration_ms)           AS p95_state,
    quantileState(0.99)(duration_ms)           AS p99_state,
    avgState(duration_ms)                      AS avg_state
FROM observability.spans
WHERE is_root = 1
GROUP BY team_id, service_name, minute;

-- Query pattern:
-- SELECT
--     service_name,
--     minute,
--     countMerge(request_count)    AS requests,
--     countIfMerge(error_count)    AS errors,
--     quantileMerge(0.95)(p95_state) AS p95_ms
-- FROM observability.spans_service_1m
-- WHERE team_id = ? AND minute BETWEEN ? AND ?
-- GROUP BY service_name, minute
-- ORDER BY minute;


-- ---------------------------------------------------------------------------
-- spans_endpoint_1m
-- Per-operation per-minute breakdown (endpoint-level granularity).
-- Feeds: endpoint metrics table, endpoint time series.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_endpoint_1m
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (team_id, service_name, operation_name, minute)
TTL minute + INTERVAL 30 DAY
AS
SELECT
    team_id,
    service_name,
    operation_name,
    http_method,
    toStartOfMinute(start_time)                AS minute,
    countState()                               AS request_count,
    countIfState(status = 'ERROR' OR http_status_code >= 400) AS error_count,
    quantileState(0.50)(duration_ms)           AS p50_state,
    quantileState(0.95)(duration_ms)           AS p95_state,
    quantileState(0.99)(duration_ms)           AS p99_state,
    avgState(duration_ms)                      AS avg_state
FROM observability.spans
WHERE is_root = 1
GROUP BY team_id, service_name, operation_name, http_method, minute;


-- ---------------------------------------------------------------------------
-- logs_level_1m
-- Per-service per-level per-minute log counts.
-- Feeds: log histogram, log volume, dashboard log overview.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_level_1m
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (team_id, service_name, level, minute)
TTL minute + INTERVAL 90 DAY
AS
SELECT
    team_id,
    service_name,
    level,
    toStartOfMinute(timestamp) AS minute,
    countState()               AS log_count
FROM observability.logs
GROUP BY team_id, service_name, level, minute;


-- ---------------------------------------------------------------------------
-- logs_service_1m
-- Per-service per-minute log counts (level-agnostic).
-- Feeds: service facets, log stats, total count queries.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_service_1m
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (team_id, service_name, minute)
TTL minute + INTERVAL 90 DAY
AS
SELECT
    team_id,
    service_name,
    toStartOfMinute(timestamp) AS minute,
    countState()               AS log_count
FROM observability.logs
GROUP BY team_id, service_name, minute;


-- ---------------------------------------------------------------------------
-- spans_edges_1m
-- Service-to-service call counts and latency — no JOIN at query time.
-- Requires parent_service_name column populated at ingestion.
-- Feeds: service dependency graph, service topology edges.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_edges_1m
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (team_id, parent_service_name, service_name, minute)
TTL minute + INTERVAL 30 DAY
AS
SELECT
    team_id,
    parent_service_name,
    service_name,
    toStartOfMinute(start_time)                               AS minute,
    countState()                                              AS call_count,
    avgState(duration_ms)                                     AS avg_latency_state,
    countIfState(status = 'ERROR' OR http_status_code >= 400) AS error_count
FROM observability.spans
WHERE parent_service_name != '' AND parent_service_name != service_name
GROUP BY team_id, parent_service_name, service_name, minute;

-- Query pattern:
-- SELECT parent_service_name AS source, service_name AS target,
--        countMerge(call_count)                                        AS call_count,
--        avgMerge(avg_latency_state)                                   AS avg_latency,
--        if(countMerge(call_count) > 0,
--           countIfMerge(error_count)*100.0/countMerge(call_count), 0)  AS error_rate
-- FROM observability.spans_edges_1m
-- WHERE team_id = ? AND minute BETWEEN ? AND ?
-- GROUP BY parent_service_name, service_name
-- ORDER BY call_count DESC LIMIT 100;
