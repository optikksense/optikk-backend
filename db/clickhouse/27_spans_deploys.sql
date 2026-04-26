-- spans_deploys -- compact deployment facts for latest-by-service and
-- deployment list reads. This keeps only the fields those endpoints need,
-- so they do not have to scan the heavier spans_version family just to
-- resolve first_seen / last_seen / span_count per deployment.

CREATE TABLE IF NOT EXISTS observability.spans_deploys_1m (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    service_name  LowCardinality(String),
    deployment_id UInt64 CODEC(T64, ZSTD(1)),
    first_seen    AggregateFunction(min, DateTime64(3)),
    last_seen     AggregateFunction(max, DateTime64(3)),
    span_count    AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, deployment_id)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_deploys_raw_to_1m
TO observability.spans_deploys_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                        AS bucket_ts,
    service_name,
    cityHash64(service_name, mat_service_version, mat_deployment_environment) AS deployment_id,
    minState(toDateTime64(timestamp, 3))                              AS first_seen,
    maxState(toDateTime64(timestamp, 3))                              AS last_seen,
    sumState(toUInt64(1))                                             AS span_count
FROM observability.spans
WHERE (parent_span_id = '' OR parent_span_id = '0000000000000000')
  AND mat_service_version != ''
GROUP BY team_id, bucket_ts, service_name, deployment_id;

CREATE TABLE IF NOT EXISTS observability.spans_deploys_5m (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    service_name  LowCardinality(String),
    deployment_id UInt64 CODEC(T64, ZSTD(1)),
    first_seen    AggregateFunction(min, DateTime64(3)),
    last_seen     AggregateFunction(max, DateTime64(3)),
    span_count    AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, deployment_id)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_deploys_1m_to_5m
TO observability.spans_deploys_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5))                AS bucket_ts,
    service_name,
    deployment_id,
    minMergeState(first_seen)                                        AS first_seen,
    maxMergeState(last_seen)                                         AS last_seen,
    sumMergeState(span_count)                                        AS span_count
FROM observability.spans_deploys_1m
GROUP BY team_id, bucket_ts, service_name, deployment_id;

CREATE TABLE IF NOT EXISTS observability.spans_deploys_1h (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    service_name  LowCardinality(String),
    deployment_id UInt64 CODEC(T64, ZSTD(1)),
    first_seen    AggregateFunction(min, DateTime64(3)),
    last_seen     AggregateFunction(max, DateTime64(3)),
    span_count    AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, deployment_id)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_deploys_5m_to_1h
TO observability.spans_deploys_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts)                                         AS bucket_ts,
    service_name,
    deployment_id,
    minMergeState(first_seen)                                        AS first_seen,
    maxMergeState(last_seen)                                         AS last_seen,
    sumMergeState(span_count)                                        AS span_count
FROM observability.spans_deploys_5m
GROUP BY team_id, bucket_ts, service_name, deployment_id;
