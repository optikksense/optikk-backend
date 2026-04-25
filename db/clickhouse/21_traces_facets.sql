-- traces_facets — HLL sketches for trace-explorer facet cardinality.
-- Previously single-tier (5m only); now extended to 1m/5m/1h so tiering
-- is uniform across all rollup families. Source: traces_index (per-trace
-- summary), not raw spans — the peer_service_set ARRAY JOIN runs on a much
-- smaller input that way.

CREATE TABLE IF NOT EXISTS observability.traces_facets_1m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    root_service      LowCardinality(String),
    root_http_method  LowCardinality(String),
    root_status       LowCardinality(String),
    trace_count       AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64),
    trace_id_hll      AggregateFunction(uniqHLL12, String),
    peer_service_hll  AggregateFunction(uniqHLL12, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, root_service, root_http_method, root_status)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.traces_facets_raw_to_1m
TO observability.traces_facets_1m AS
SELECT
    team_id,
    toStartOfMinute(toDateTime64(start_ms / 1000.0, 3)) AS bucket_ts,
    root_service,
    root_http_method,
    root_status,
    sumState(toUInt64(1))                          AS trace_count,
    sumState(toUInt64(has_error))                  AS error_count,
    uniqHLL12State(trace_id)                       AS trace_id_hll,
    uniqHLL12State(arrayJoin(peer_service_set))    AS peer_service_hll
FROM observability.traces_index
GROUP BY team_id, bucket_ts, root_service, root_http_method, root_status;

CREATE TABLE IF NOT EXISTS observability.traces_facets_5m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    root_service      LowCardinality(String),
    root_http_method  LowCardinality(String),
    root_status       LowCardinality(String),
    trace_count       AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64),
    trace_id_hll      AggregateFunction(uniqHLL12, String),
    peer_service_hll  AggregateFunction(uniqHLL12, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, root_service, root_http_method, root_status)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.traces_facets_1m_to_5m
TO observability.traces_facets_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    root_service, root_http_method, root_status,
    sumMergeState(trace_count)         AS trace_count,
    sumMergeState(error_count)         AS error_count,
    uniqHLL12MergeState(trace_id_hll)      AS trace_id_hll,
    uniqHLL12MergeState(peer_service_hll)  AS peer_service_hll
FROM observability.traces_facets_1m
GROUP BY team_id, bucket_ts, root_service, root_http_method, root_status;

CREATE TABLE IF NOT EXISTS observability.traces_facets_1h (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    root_service      LowCardinality(String),
    root_http_method  LowCardinality(String),
    root_status       LowCardinality(String),
    trace_count       AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64),
    trace_id_hll      AggregateFunction(uniqHLL12, String),
    peer_service_hll  AggregateFunction(uniqHLL12, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, root_service, root_http_method, root_status)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.traces_facets_5m_to_1h
TO observability.traces_facets_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    root_service, root_http_method, root_status,
    sumMergeState(trace_count)         AS trace_count,
    sumMergeState(error_count)         AS error_count,
    uniqHLL12MergeState(trace_id_hll)      AS trace_id_hll,
    uniqHLL12MergeState(peer_service_hll)  AS peer_service_hll
FROM observability.traces_facets_5m
GROUP BY team_id, bucket_ts, root_service, root_http_method, root_status;
