-- spans_rollup_5m
CREATE TABLE IF NOT EXISTS observability.spans_rollup_5m (
    team_id          UInt32            CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime          CODEC(DoubleDelta, LZ4),
    service_name     LowCardinality(String),
    operation_name   LowCardinality(String),
    endpoint         LowCardinality(String),
    http_method      LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count    AggregateFunction(sum, UInt64),
    error_count      AggregateFunction(sum, UInt64),
    duration_ms_sum  AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, endpoint, http_method)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_rollup_1m_to_5m
TO observability.spans_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       service_name, operation_name, endpoint, http_method,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)   AS request_count,
       sumMergeState(error_count)     AS error_count,
       sumMergeState(duration_ms_sum) AS duration_ms_sum
FROM observability.spans_rollup_1m
GROUP BY team_id, bucket_ts, service_name, operation_name, endpoint, http_method;

CREATE TABLE IF NOT EXISTS observability.spans_rollup_1h (
    team_id          UInt32            CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime          CODEC(DoubleDelta, LZ4),
    service_name     LowCardinality(String),
    operation_name   LowCardinality(String),
    endpoint         LowCardinality(String),
    http_method      LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count    AggregateFunction(sum, UInt64),
    error_count      AggregateFunction(sum, UInt64),
    duration_ms_sum  AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, endpoint, http_method)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_rollup_5m_to_1h
TO observability.spans_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       service_name, operation_name, endpoint, http_method,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)   AS request_count,
       sumMergeState(error_count)     AS error_count,
       sumMergeState(duration_ms_sum) AS duration_ms_sum
FROM observability.spans_rollup_5m
GROUP BY team_id, bucket_ts, service_name, operation_name, endpoint, http_method;

-- metrics_histograms_rollup_5m + _1h
CREATE TABLE IF NOT EXISTS observability.metrics_histograms_rollup_5m (
    team_id      UInt32           CODEC(T64, ZSTD(1)),
    bucket_ts    DateTime         CODEC(DoubleDelta, LZ4),
    metric_name  LowCardinality(String),
    service      LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count   AggregateFunction(sum, UInt64),
    hist_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_histograms_rollup_1m_to_5m
TO observability.metrics_histograms_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.metrics_histograms_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service;

CREATE TABLE IF NOT EXISTS observability.metrics_histograms_rollup_1h (
    team_id      UInt32           CODEC(T64, ZSTD(1)),
    bucket_ts    DateTime         CODEC(DoubleDelta, LZ4),
    metric_name  LowCardinality(String),
    service      LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count   AggregateFunction(sum, UInt64),
    hist_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_histograms_rollup_5m_to_1h
TO observability.metrics_histograms_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.metrics_histograms_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service;

-- logs_rollup_5m + _1h
CREATE TABLE IF NOT EXISTS observability.logs_rollup_5m (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    severity_text LowCardinality(String),
    service       LowCardinality(String),
    host          LowCardinality(String),
    pod           LowCardinality(String),
    log_count     AggregateFunction(sum, UInt64),
    error_count   AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, severity_text, service, host, pod)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_rollup_1m_to_5m
TO observability.logs_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       severity_text, service, host, pod,
       sumMergeState(log_count)   AS log_count,
       sumMergeState(error_count) AS error_count
FROM observability.logs_rollup_1m
GROUP BY team_id, bucket_ts, severity_text, service, host, pod;

CREATE TABLE IF NOT EXISTS observability.logs_rollup_1h (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    severity_text LowCardinality(String),
    service       LowCardinality(String),
    host          LowCardinality(String),
    pod           LowCardinality(String),
    log_count     AggregateFunction(sum, UInt64),
    error_count   AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, severity_text, service, host, pod)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_rollup_5m_to_1h
TO observability.logs_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       severity_text, service, host, pod,
       sumMergeState(log_count)   AS log_count,
       sumMergeState(error_count) AS error_count
FROM observability.logs_rollup_5m
GROUP BY team_id, bucket_ts, severity_text, service, host, pod;

-- ai_spans_rollup_5m + _1h
CREATE TABLE IF NOT EXISTS observability.ai_spans_rollup_5m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    ai_system      LowCardinality(String),
    ai_model       LowCardinality(String),
    ai_operation   LowCardinality(String),
    service_name   LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    input_tokens_sum   AggregateFunction(sum, UInt64),
    output_tokens_sum  AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.ai_spans_rollup_1m_to_5m
TO observability.ai_spans_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       ai_system, ai_model, ai_operation, service_name,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)     AS request_count,
       sumMergeState(error_count)       AS error_count,
       sumMergeState(input_tokens_sum)  AS input_tokens_sum,
       sumMergeState(output_tokens_sum) AS output_tokens_sum
FROM observability.ai_spans_rollup_1m
GROUP BY team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name;

CREATE TABLE IF NOT EXISTS observability.ai_spans_rollup_1h (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    ai_system      LowCardinality(String),
    ai_model       LowCardinality(String),
    ai_operation   LowCardinality(String),
    service_name   LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    input_tokens_sum   AggregateFunction(sum, UInt64),
    output_tokens_sum  AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.ai_spans_rollup_5m_to_1h
TO observability.ai_spans_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       ai_system, ai_model, ai_operation, service_name,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)     AS request_count,
       sumMergeState(error_count)       AS error_count,
       sumMergeState(input_tokens_sum)  AS input_tokens_sum,
       sumMergeState(output_tokens_sum) AS output_tokens_sum
FROM observability.ai_spans_rollup_5m
GROUP BY team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name;

-- spans_error_fingerprint_5m + _1h
CREATE TABLE IF NOT EXISTS observability.spans_error_fingerprint_5m (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts            DateTime CODEC(DoubleDelta, LZ4),
    service_name         LowCardinality(String),
    operation_name       LowCardinality(String),
    exception_type       LowCardinality(String),
    status_message_hash  UInt64 CODEC(T64, ZSTD(1)),
    http_status_bucket   LowCardinality(String),
    error_count          AggregateFunction(sum, UInt64),
    sample_trace_id      AggregateFunction(any, String),
    sample_status_message AggregateFunction(any, String),
    first_seen           AggregateFunction(min, DateTime64(9)),
    last_seen            AggregateFunction(max, DateTime64(9))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, exception_type, status_message_hash, http_status_bucket)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_error_fingerprint_1m_to_5m
TO observability.spans_error_fingerprint_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       service_name, operation_name, exception_type, status_message_hash, http_status_bucket,
       sumMergeState(error_count)                AS error_count,
       anyMergeState(sample_trace_id)            AS sample_trace_id,
       anyMergeState(sample_status_message)      AS sample_status_message,
       minMergeState(first_seen)                 AS first_seen,
       maxMergeState(last_seen)                  AS last_seen
FROM observability.spans_error_fingerprint_1m
GROUP BY team_id, bucket_ts, service_name, operation_name, exception_type, status_message_hash, http_status_bucket;

CREATE TABLE IF NOT EXISTS observability.spans_error_fingerprint_1h (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts            DateTime CODEC(DoubleDelta, LZ4),
    service_name         LowCardinality(String),
    operation_name       LowCardinality(String),
    exception_type       LowCardinality(String),
    status_message_hash  UInt64 CODEC(T64, ZSTD(1)),
    http_status_bucket   LowCardinality(String),
    error_count          AggregateFunction(sum, UInt64),
    sample_trace_id      AggregateFunction(any, String),
    sample_status_message AggregateFunction(any, String),
    first_seen           AggregateFunction(min, DateTime64(9)),
    last_seen            AggregateFunction(max, DateTime64(9))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, exception_type, status_message_hash, http_status_bucket)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_error_fingerprint_5m_to_1h
TO observability.spans_error_fingerprint_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       service_name, operation_name, exception_type, status_message_hash, http_status_bucket,
       sumMergeState(error_count)                AS error_count,
       anyMergeState(sample_trace_id)            AS sample_trace_id,
       anyMergeState(sample_status_message)      AS sample_status_message,
       minMergeState(first_seen)                 AS first_seen,
       maxMergeState(last_seen)                  AS last_seen
FROM observability.spans_error_fingerprint_5m
GROUP BY team_id, bucket_ts, service_name, operation_name, exception_type, status_message_hash, http_status_bucket;

-- spans_host_rollup_5m + _1h
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

-- spans_by_version_5m + _1h
CREATE TABLE IF NOT EXISTS observability.spans_by_version_5m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    service_name    LowCardinality(String),
    service_version LowCardinality(String),
    environment     LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    first_seen         AggregateFunction(min, DateTime64(9)),
    last_seen          AggregateFunction(max, DateTime64(9)),
    commit_sha         AggregateFunction(any, String),
    commit_author      AggregateFunction(any, String),
    repo_url           AggregateFunction(any, String),
    pr_url             AggregateFunction(any, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, service_version, environment)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_by_version_1m_to_5m
TO observability.spans_by_version_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       service_name, service_version, environment,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)  AS request_count,
       sumMergeState(error_count)    AS error_count,
       minMergeState(first_seen)     AS first_seen,
       maxMergeState(last_seen)      AS last_seen,
       anyMergeState(commit_sha)     AS commit_sha,
       anyMergeState(commit_author)  AS commit_author,
       anyMergeState(repo_url)       AS repo_url,
       anyMergeState(pr_url)         AS pr_url
FROM observability.spans_by_version_1m
GROUP BY team_id, bucket_ts, service_name, service_version, environment;

CREATE TABLE IF NOT EXISTS observability.spans_by_version_1h (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    service_name    LowCardinality(String),
    service_version LowCardinality(String),
    environment     LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    first_seen         AggregateFunction(min, DateTime64(9)),
    last_seen          AggregateFunction(max, DateTime64(9)),
    commit_sha         AggregateFunction(any, String),
    commit_author      AggregateFunction(any, String),
    repo_url           AggregateFunction(any, String),
    pr_url             AggregateFunction(any, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, service_version, environment)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_by_version_5m_to_1h
TO observability.spans_by_version_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       service_name, service_version, environment,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)  AS request_count,
       sumMergeState(error_count)    AS error_count,
       minMergeState(first_seen)     AS first_seen,
       maxMergeState(last_seen)      AS last_seen,
       anyMergeState(commit_sha)     AS commit_sha,
       anyMergeState(commit_author)  AS commit_author,
       anyMergeState(repo_url)       AS repo_url,
       anyMergeState(pr_url)         AS pr_url
FROM observability.spans_by_version_5m
GROUP BY team_id, bucket_ts, service_name, service_version, environment;
