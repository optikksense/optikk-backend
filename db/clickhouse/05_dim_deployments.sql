-- observability.deployments — dimension table holding VCS metadata per
-- (service, version, environment) triple. Replaces the embedded VCS
-- strings that used to live on spans_by_version_rollup (commit_sha,
-- commit_author, repo_url, pr_url as AggregateFunction(any, String)
-- inflated every tier's row size). Rollups now store only deployment_id;
-- readers join here when they need display metadata.
--
-- deployment_id derivation is stable: cityHash64(service_name, service_version, environment).
-- Computed by the MV below so the Go mapper doesn't need to know the hash scheme.
-- ReplacingMergeTree keyed on the natural tuple lets late rows update metadata
-- (e.g. pr_url arriving after first ingest) without duplicating.

CREATE TABLE IF NOT EXISTS observability.deployments (
    deployment_id    UInt64                 CODEC(T64, ZSTD(1)),
    team_id          UInt32                 CODEC(T64, ZSTD(1)),
    service_name     LowCardinality(String) CODEC(ZSTD(1)),
    service_version  String                 CODEC(ZSTD(1)),
    environment      LowCardinality(String) CODEC(ZSTD(1)),
    commit_sha       String                 CODEC(ZSTD(1)),
    commit_author    String                 CODEC(ZSTD(1)),
    repo_url         String                 CODEC(ZSTD(1)),
    pr_url           String                 CODEC(ZSTD(1)),
    first_seen       DateTime64(3)          CODEC(DoubleDelta, LZ4),
    last_seen        DateTime64(3)          CODEC(DoubleDelta, LZ4),
    INDEX idx_deployment_id deployment_id TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = ReplacingMergeTree(last_seen)
PARTITION BY toYYYYMM(first_seen)
ORDER BY (team_id, service_name, service_version, environment)
TTL toDateTime(last_seen) + INTERVAL 180 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

-- Populate from root spans. One row per (service, version, env) combo; late
-- spans with richer VCS tags update the row (ReplacingMergeTree picks the
-- max last_seen on merge).
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_to_deployments
TO observability.deployments AS
SELECT
    cityHash64(service_name, mat_service_version, mat_deployment_environment) AS deployment_id,
    team_id,
    service_name,
    mat_service_version                                                        AS service_version,
    mat_deployment_environment                                                 AS environment,
    CAST(attributes.`vcs.repository.ref.revision`, 'String')                   AS commit_sha,
    CAST(attributes.`vcs.repository.author`, 'String')                         AS commit_author,
    CAST(attributes.`vcs.repository.url.full`, 'String')                       AS repo_url,
    CAST(attributes.`vcs.change.url.full`, 'String')                           AS pr_url,
    timestamp                                                                  AS first_seen,
    timestamp                                                                  AS last_seen
FROM observability.signoz_index_v3
WHERE is_root = 1
  AND mat_service_version != '';
