-- Deployments dimension table — VCS metadata per (service, version, environment); deployment_id = cityHash64(service, service_version, environment) computed by the MV.

CREATE TABLE IF NOT EXISTS observability.deployments (
    deployment_id    UInt64                 CODEC(T64, ZSTD(1)),
    team_id          UInt32                 CODEC(T64, ZSTD(1)),
    service          LowCardinality(String) CODEC(ZSTD(1)),
    service_version  String                 CODEC(ZSTD(1)),
    environment      LowCardinality(String) CODEC(ZSTD(1)),
    commit_sha       String                 CODEC(ZSTD(1)),
    commit_author    String                 CODEC(ZSTD(1)),
    repo_url         String                 CODEC(ZSTD(1)),
    pr_url           String                 CODEC(ZSTD(1)),
    first_seen       DateTime64(3)          CODEC(DoubleDelta, LZ4),
    last_seen        DateTime64(3)          CODEC(DoubleDelta, LZ4)
) ENGINE = ReplacingMergeTree(last_seen)
PARTITION BY toYYYYMM(first_seen)
ORDER BY (team_id, service, service_version, environment)
TTL toDateTime(last_seen) + INTERVAL 180 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_to_deployments
TO observability.deployments AS
SELECT
    cityHash64(service, service_version, environment)               AS deployment_id,
    team_id,
    service,
    service_version,
    environment,
    CAST(attributes.`vcs.repository.ref.revision`, 'String')        AS commit_sha,
    CAST(attributes.`vcs.repository.author`, 'String')              AS commit_author,
    CAST(attributes.`vcs.repository.url.full`, 'String')            AS repo_url,
    CAST(attributes.`vcs.change.url.full`, 'String')                AS pr_url,
    timestamp                                                        AS first_seen,
    timestamp                                                        AS last_seen
FROM observability.spans
WHERE is_root = 1
  AND service_version != ''
  AND attributes.`vcs.repository.ref.revision` != '';
