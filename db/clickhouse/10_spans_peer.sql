-- spans_peer — CLIENT-side latency/error breakdown per peer service/host/status.
-- Source: raw observability.spans with kind=CLIENT.
-- PK realigned from the pre-rewrite ordering: http_status_bucket moves ahead
-- of peer_service + host_name because dashboards filter by service+status
-- first, then drill into peer. With this PK, error-only queries (status=5xx)
-- prune ~99% of rows.

CREATE TABLE IF NOT EXISTS observability.spans_peer_1m (
    team_id             UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts           DateTime CODEC(DoubleDelta, LZ4),
    service_name        LowCardinality(String),
    http_status_bucket  LowCardinality(String),
    peer_service        LowCardinality(String),
    host_name           LowCardinality(String),
    latency_ms_digest   AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count       AggregateFunction(sum, UInt64),
    error_count         AggregateFunction(sum, UInt64),
    duration_ms_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, http_status_bucket, peer_service, host_name)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_peer_raw_to_1m
TO observability.spans_peer_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp) AS bucket_ts,
    service_name,
    http_status_bucket,
    mat_peer_service           AS peer_service,
    mat_host_name              AS host_name,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1e6, toUInt64(1)) AS latency_ms_digest,
    sumState(toUInt64(1))                                                            AS request_count,
    sumState(toUInt64(has_error))                                                    AS error_count,
    sumState(duration_nano / 1e6)                                                    AS duration_ms_sum
FROM observability.spans
WHERE kind = 3
GROUP BY team_id, bucket_ts, service_name, http_status_bucket, peer_service, host_name;

CREATE TABLE IF NOT EXISTS observability.spans_peer_5m (
    team_id             UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts           DateTime CODEC(DoubleDelta, LZ4),
    service_name        LowCardinality(String),
    http_status_bucket  LowCardinality(String),
    peer_service        LowCardinality(String),
    host_name           LowCardinality(String),
    latency_ms_digest   AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count       AggregateFunction(sum, UInt64),
    error_count         AggregateFunction(sum, UInt64),
    duration_ms_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, http_status_bucket, peer_service, host_name)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_peer_1m_to_5m
TO observability.spans_peer_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    service_name, http_status_bucket, peer_service, host_name,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(request_count)                                           AS request_count,
    sumMergeState(error_count)                                             AS error_count,
    sumMergeState(duration_ms_sum)                                         AS duration_ms_sum
FROM observability.spans_peer_1m
GROUP BY team_id, bucket_ts, service_name, http_status_bucket, peer_service, host_name;

CREATE TABLE IF NOT EXISTS observability.spans_peer_1h (
    team_id             UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts           DateTime CODEC(DoubleDelta, LZ4),
    service_name        LowCardinality(String),
    http_status_bucket  LowCardinality(String),
    peer_service        LowCardinality(String),
    host_name           LowCardinality(String),
    latency_ms_digest   AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count       AggregateFunction(sum, UInt64),
    error_count         AggregateFunction(sum, UInt64),
    duration_ms_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, http_status_bucket, peer_service, host_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_peer_5m_to_1h
TO observability.spans_peer_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    service_name, http_status_bucket, peer_service, host_name,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(request_count)                                           AS request_count,
    sumMergeState(error_count)                                             AS error_count,
    sumMergeState(duration_ms_sum)                                         AS duration_ms_sum
FROM observability.spans_peer_5m
GROUP BY team_id, bucket_ts, service_name, http_status_bucket, peer_service, host_name;
