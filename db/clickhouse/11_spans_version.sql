-- spans_version — per-deployment RED stats for version-over-version comparison
-- and deploy correlation. VCS metadata (commit_sha, repo_url, pr_url) lives
-- in observability.deployments (see 05_dim_deployments.sql); this rollup
-- stores only deployment_id and joins at read time.
--
-- Source: raw spans, root spans only (parent_span_id empty / zero), filtered
-- to rows that carry service.version. deployment_id is computed the same way
-- as in the deployments MV so the two always match.

CREATE TABLE IF NOT EXISTS observability.spans_version_1m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    service_name      LowCardinality(String),
    deployment_id     UInt64 CODEC(T64, ZSTD(1)),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64),
    first_seen        AggregateFunction(min, DateTime64(3)),
    last_seen         AggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, deployment_id)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_version_raw_to_1m
TO observability.spans_version_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                   AS bucket_ts,
    service_name,
    cityHash64(service_name, mat_service_version, mat_deployment_environment)    AS deployment_id,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1e6, toUInt64(1)) AS latency_ms_digest,
    sumState(toUInt64(1))                                                        AS request_count,
    sumState(toUInt64(has_error))                                                AS error_count,
    minState(timestamp)                                                          AS first_seen,
    maxState(timestamp)                                                          AS last_seen
FROM observability.spans
WHERE (parent_span_id = '' OR parent_span_id = '0000000000000000')
  AND mat_service_version != ''
GROUP BY team_id, bucket_ts, service_name, deployment_id;

CREATE TABLE IF NOT EXISTS observability.spans_version_5m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    service_name      LowCardinality(String),
    deployment_id     UInt64 CODEC(T64, ZSTD(1)),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64),
    first_seen        AggregateFunction(min, DateTime64(3)),
    last_seen         AggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, deployment_id)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_version_1m_to_5m
TO observability.spans_version_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    service_name, deployment_id,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(request_count) AS request_count,
    sumMergeState(error_count)   AS error_count,
    minMergeState(first_seen)    AS first_seen,
    maxMergeState(last_seen)     AS last_seen
FROM observability.spans_version_1m
GROUP BY team_id, bucket_ts, service_name, deployment_id;

CREATE TABLE IF NOT EXISTS observability.spans_version_1h (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    service_name      LowCardinality(String),
    deployment_id     UInt64 CODEC(T64, ZSTD(1)),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64),
    first_seen        AggregateFunction(min, DateTime64(3)),
    last_seen         AggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, deployment_id)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_version_5m_to_1h
TO observability.spans_version_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    service_name, deployment_id,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(request_count) AS request_count,
    sumMergeState(error_count)   AS error_count,
    minMergeState(first_seen)    AS first_seen,
    maxMergeState(last_seen)     AS last_seen
FROM observability.spans_version_5m
GROUP BY team_id, bucket_ts, service_name, deployment_id;
