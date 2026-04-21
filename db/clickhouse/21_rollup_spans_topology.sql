-- spans_topology_rollup — service-to-service edges from CLIENT-kind spans.
-- Source: span.service_name; target: mat_peer_service; operation: span.name.
-- Single-pass, no self-join. Powers: services/topology GetEdges. (GetNodes
-- uses the per-service spans_rollup.)
-- Edges whose producer doesn't populate peer.service are missed — document
-- the caveat on PR and consider a two-pass edge job later if needed.

CREATE TABLE IF NOT EXISTS observability.spans_topology_rollup_1m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    client_service  LowCardinality(String),
    server_service  LowCardinality(String),
    operation       LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, client_service, server_service, operation)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_topology_to_rollup_1m
TO observability.spans_topology_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    service_name                                                                           AS client_service,
    mat_peer_service                                                                       AS server_service,
    name                                                                                   AS operation,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1000000.0, toUInt64(1)) AS latency_ms_digest,
    sumState(toUInt64(1))                                                                  AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))           AS error_count
FROM observability.spans
WHERE kind = 3 AND mat_peer_service != ''; -- SPAN_KIND_CLIENT = 3

CREATE TABLE IF NOT EXISTS observability.spans_topology_rollup_5m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    client_service  LowCardinality(String),
    server_service  LowCardinality(String),
    operation       LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, client_service, server_service, operation)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_topology_rollup_1m_to_5m
TO observability.spans_topology_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       client_service, server_service, operation,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count) AS request_count,
       sumMergeState(error_count)   AS error_count
FROM observability.spans_topology_rollup_1m
GROUP BY team_id, bucket_ts, client_service, server_service, operation;

CREATE TABLE IF NOT EXISTS observability.spans_topology_rollup_1h (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    client_service  LowCardinality(String),
    server_service  LowCardinality(String),
    operation       LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, client_service, server_service, operation)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_topology_rollup_5m_to_1h
TO observability.spans_topology_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       client_service, server_service, operation,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count) AS request_count,
       sumMergeState(error_count)   AS error_count
FROM observability.spans_topology_rollup_5m
GROUP BY team_id, bucket_ts, client_service, server_service, operation;
