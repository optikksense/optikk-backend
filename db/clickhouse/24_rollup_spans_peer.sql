-- spans_peer_rollup — external-host / peer-service aggregates for CLIENT-kind
-- spans. Distinct from `spans_topology_rollup` because topology keys edges
-- only; this rollup carries latency digest + http_status_bucket needed for the
-- HTTP external-host panels.
-- Powers: overview/httpmetrics GetTopExternalHosts / GetExternalHostLatency /
-- GetExternalHostErrorRate.

CREATE TABLE IF NOT EXISTS observability.spans_peer_rollup_1m (
    team_id            UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts          DateTime CODEC(DoubleDelta, LZ4),
    service_name       LowCardinality(String),
    peer_service       LowCardinality(String),
    host_name          LowCardinality(String),
    http_status_bucket LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    duration_ms_sum    AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, peer_service, host_name, http_status_bucket)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_peer_to_rollup_1m
TO observability.spans_peer_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    service_name                                                                           AS service_name,
    mat_peer_service                                                                       AS peer_service,
    http_host                                                                              AS host_name,
    multiIf(
        toUInt16OrZero(response_status_code) BETWEEN 200 AND 299, '2xx',
        toUInt16OrZero(response_status_code) BETWEEN 300 AND 399, '3xx',
        toUInt16OrZero(response_status_code) BETWEEN 400 AND 499, '4xx',
        toUInt16OrZero(response_status_code) >= 500, '5xx',
        has_error, 'err',
        'other'
    )                                                                                      AS http_status_bucket,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1000000.0, toUInt64(1)) AS latency_ms_digest,
    sumState(toUInt64(1))                                                                  AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))           AS error_count,
    sumState(duration_nano / 1000000.0)                                                    AS duration_ms_sum
FROM observability.spans
WHERE kind = 3; -- SPAN_KIND_CLIENT

CREATE TABLE IF NOT EXISTS observability.spans_peer_rollup_5m (
    team_id            UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts          DateTime CODEC(DoubleDelta, LZ4),
    service_name       LowCardinality(String),
    peer_service       LowCardinality(String),
    host_name          LowCardinality(String),
    http_status_bucket LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    duration_ms_sum    AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, peer_service, host_name, http_status_bucket)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_peer_rollup_1m_to_5m
TO observability.spans_peer_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       service_name, peer_service, host_name, http_status_bucket,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)   AS request_count,
       sumMergeState(error_count)     AS error_count,
       sumMergeState(duration_ms_sum) AS duration_ms_sum
FROM observability.spans_peer_rollup_1m
GROUP BY team_id, bucket_ts, service_name, peer_service, host_name, http_status_bucket;

CREATE TABLE IF NOT EXISTS observability.spans_peer_rollup_1h (
    team_id            UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts          DateTime CODEC(DoubleDelta, LZ4),
    service_name       LowCardinality(String),
    peer_service       LowCardinality(String),
    host_name          LowCardinality(String),
    http_status_bucket LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    duration_ms_sum    AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, peer_service, host_name, http_status_bucket)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_peer_rollup_5m_to_1h
TO observability.spans_peer_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       service_name, peer_service, host_name, http_status_bucket,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)   AS request_count,
       sumMergeState(error_count)     AS error_count,
       sumMergeState(duration_ms_sum) AS duration_ms_sum
FROM observability.spans_peer_rollup_5m
GROUP BY team_id, bucket_ts, service_name, peer_service, host_name, http_status_bucket;
