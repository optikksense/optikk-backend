-- spans_errors — error-fingerprint rollup. Groups spans by (service, operation,
-- exception_type, status_message_hash, http_status_bucket) to surface
-- recurring error signatures. Source: raw spans where has_error OR
-- response_status_code >= 400. Carries a sampled trace_id + status_message
-- per bucket so the UI can link to an exemplar.

CREATE TABLE IF NOT EXISTS observability.spans_errors_1m (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts            DateTime CODEC(DoubleDelta, LZ4),
    service_name         LowCardinality(String),
    http_status_bucket   LowCardinality(String),
    exception_type       LowCardinality(String),
    operation_name       LowCardinality(String),
    status_message_hash  UInt64 CODEC(T64, ZSTD(1)),
    error_count          AggregateFunction(sum, UInt64),
    sample_trace_id      AggregateFunction(any, String),
    sample_status_message AggregateFunction(any, String),
    first_seen           AggregateFunction(min, DateTime64(3)),
    last_seen            AggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, http_status_bucket, exception_type, operation_name, status_message_hash)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_errors_raw_to_1m
TO observability.spans_errors_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp) AS bucket_ts,
    service_name,
    http_status_bucket,
    mat_exception_type         AS exception_type,
    name                       AS operation_name,
    cityHash64(status_message) AS status_message_hash,
    sumState(toUInt64(1))      AS error_count,
    anyState(trace_id)         AS sample_trace_id,
    anyState(status_message)   AS sample_status_message,
    minState(timestamp)        AS first_seen,
    maxState(timestamp)        AS last_seen
FROM observability.spans
WHERE has_error = true OR toUInt16OrZero(response_status_code) >= 400
GROUP BY team_id, bucket_ts, service_name, http_status_bucket, exception_type, operation_name, status_message_hash;

CREATE TABLE IF NOT EXISTS observability.spans_errors_5m (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts            DateTime CODEC(DoubleDelta, LZ4),
    service_name         LowCardinality(String),
    http_status_bucket   LowCardinality(String),
    exception_type       LowCardinality(String),
    operation_name       LowCardinality(String),
    status_message_hash  UInt64 CODEC(T64, ZSTD(1)),
    error_count          AggregateFunction(sum, UInt64),
    sample_trace_id      AggregateFunction(any, String),
    sample_status_message AggregateFunction(any, String),
    first_seen           AggregateFunction(min, DateTime64(3)),
    last_seen            AggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, http_status_bucket, exception_type, operation_name, status_message_hash)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_errors_1m_to_5m
TO observability.spans_errors_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    service_name, http_status_bucket, exception_type, operation_name, status_message_hash,
    sumMergeState(error_count)          AS error_count,
    anyMergeState(sample_trace_id)      AS sample_trace_id,
    anyMergeState(sample_status_message) AS sample_status_message,
    minMergeState(first_seen)           AS first_seen,
    maxMergeState(last_seen)            AS last_seen
FROM observability.spans_errors_1m
GROUP BY team_id, bucket_ts, service_name, http_status_bucket, exception_type, operation_name, status_message_hash;

CREATE TABLE IF NOT EXISTS observability.spans_errors_1h (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts            DateTime CODEC(DoubleDelta, LZ4),
    service_name         LowCardinality(String),
    http_status_bucket   LowCardinality(String),
    exception_type       LowCardinality(String),
    operation_name       LowCardinality(String),
    status_message_hash  UInt64 CODEC(T64, ZSTD(1)),
    error_count          AggregateFunction(sum, UInt64),
    sample_trace_id      AggregateFunction(any, String),
    sample_status_message AggregateFunction(any, String),
    first_seen           AggregateFunction(min, DateTime64(3)),
    last_seen            AggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, http_status_bucket, exception_type, operation_name, status_message_hash)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_errors_5m_to_1h
TO observability.spans_errors_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    service_name, http_status_bucket, exception_type, operation_name, status_message_hash,
    sumMergeState(error_count)          AS error_count,
    anyMergeState(sample_trace_id)      AS sample_trace_id,
    anyMergeState(sample_status_message) AS sample_status_message,
    minMergeState(first_seen)           AS first_seen,
    maxMergeState(last_seen)            AS last_seen
FROM observability.spans_errors_5m
GROUP BY team_id, bucket_ts, service_name, http_status_bucket, exception_type, operation_name, status_message_hash;
