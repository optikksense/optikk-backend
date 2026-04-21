-- spans_by_version — root-span RED per (service, version, environment) plus
-- sampled VCS metadata (commit sha / author / repo / pr). Powers: deployments
-- module version-over-version comparisons + alerting deploy correlation.

CREATE TABLE IF NOT EXISTS observability.spans_by_version_1m (
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

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_by_version_to_1m
TO observability.spans_by_version_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                                 AS bucket_ts,
    service_name                                                                               AS service_name,
    mat_service_version                                                                        AS service_version,
    mat_deployment_environment                                                                 AS environment,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1000000.0, toUInt64(1))     AS latency_ms_digest,
    sumState(toUInt64(1))                                                                      AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))               AS error_count,
    minState(timestamp)                                                                        AS first_seen,
    maxState(timestamp)                                                                        AS last_seen,
    anyState(attributes.`vcs.commit.sha`::String)                                              AS commit_sha,
    anyState(attributes.`vcs.commit.author`::String)                                           AS commit_author,
    anyState(attributes.`vcs.repository.url`::String)                                          AS repo_url,
    anyState(attributes.`vcs.pr.url`::String)                                                  AS pr_url
FROM observability.spans
WHERE mat_service_version != ''
  AND (parent_span_id = '' OR parent_span_id = '0000000000000000');

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
