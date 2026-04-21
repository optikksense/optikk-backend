-- spans_host_rollup — RED per (host, pod, service). All spans, not just root.
-- Powers: infrastructure host/pod dashboards that correlate with span traffic.

CREATE TABLE IF NOT EXISTS observability.spans_host_rollup_1m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    host_name       LowCardinality(String),
    pod_name        LowCardinality(String),
    service_name    LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    duration_ms_sum    AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, host_name, pod_name, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_host_to_rollup_1m
TO observability.spans_host_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                                 AS bucket_ts,
    mat_host_name                                                                              AS host_name,
    mat_k8s_pod_name                                                                           AS pod_name,
    service_name                                                                               AS service_name,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1000000.0, toUInt64(1))     AS latency_ms_digest,
    sumState(toUInt64(1))                                                                      AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))               AS error_count,
    sumState(duration_nano / 1000000.0)                                                        AS duration_ms_sum
FROM observability.spans
WHERE mat_host_name != '' OR mat_k8s_pod_name != '';

CREATE TABLE IF NOT EXISTS observability.spans_host_rollup_5m (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    host_name     LowCardinality(String),
    pod_name      LowCardinality(String),
    service_name  LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    duration_ms_sum    AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, host_name, pod_name, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_host_rollup_1m_to_5m
TO observability.spans_host_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       host_name, pod_name, service_name,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)   AS request_count,
       sumMergeState(error_count)     AS error_count,
       sumMergeState(duration_ms_sum) AS duration_ms_sum
FROM observability.spans_host_rollup_1m
GROUP BY team_id, bucket_ts, host_name, pod_name, service_name;

CREATE TABLE IF NOT EXISTS observability.spans_host_rollup_1h (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    host_name     LowCardinality(String),
    pod_name      LowCardinality(String),
    service_name  LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    duration_ms_sum    AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, host_name, pod_name, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_host_rollup_5m_to_1h
TO observability.spans_host_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       host_name, pod_name, service_name,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)   AS request_count,
       sumMergeState(error_count)     AS error_count,
       sumMergeState(duration_ms_sum) AS duration_ms_sum
FROM observability.spans_host_rollup_5m
GROUP BY team_id, bucket_ts, host_name, pod_name, service_name;
