-- Legacy spans pipeline cutover. Three concerns, in order:
--   1. Drop the old spans_rollup_{1m,5m,1h} cascade (replaced by
--      spans_rollup_v2_* sourced from traces_index in 39_rollup_spans_v2.sql).
--   2. Repoint the secondary rollup MVs — topology, error_fingerprint, host,
--      by_version, peer, kind — at observability.spans_v2. These rollups are
--      consumed by other modules (overview topology, APM error groups, etc)
--      per plan §B.3, so we keep the target tables intact and only swap the
--      source MV definitions. We DROP the source MV then recreate it with
--      identical SELECT shape but FROM observability.spans_v2.
--   3. Drop the dual-write MV and the raw observability.spans table.
--
-- Pattern mirrors 26_drop_legacy_metrics_gauges_v2_objects.sql and
-- 29_drop_ai_spans_rollup_objects.sql (MVs first, then tables).

-- 1. Drop legacy spans_rollup_* cascade (tables + MVs).
DROP TABLE IF EXISTS observability.spans_rollup_5m_to_1h;
DROP TABLE IF EXISTS observability.spans_rollup_1m_to_5m;
DROP TABLE IF EXISTS observability.spans_to_rollup_1m;

DROP TABLE IF EXISTS observability.spans_rollup_1h;
DROP TABLE IF EXISTS observability.spans_rollup_5m;
DROP TABLE IF EXISTS observability.spans_rollup_1m;

-- 2a. Repoint spans_topology_to_rollup_1m at spans_v2.
DROP TABLE IF EXISTS observability.spans_topology_to_rollup_1m;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_topology_to_rollup_1m
TO observability.spans_topology_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    service_name                                                                           AS client_service,
    mat_peer_service                                                                       AS server_service,
    name                                                                                   AS operation,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1000000.0, toUInt64(1)) AS latency_ms_digest,
    sumState(toUInt64(1))                                                                  AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))           AS error_count
FROM observability.spans_v2
WHERE kind = 3 AND mat_peer_service != ''
GROUP BY team_id, bucket_ts, client_service, server_service, operation;

-- 2b. Repoint spans_error_fingerprint_to_1m at spans_v2.
DROP TABLE IF EXISTS observability.spans_error_fingerprint_to_1m;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_error_fingerprint_to_1m
TO observability.spans_error_fingerprint_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                                 AS bucket_ts,
    service_name                                                                               AS service_name,
    name                                                                                       AS operation_name,
    mat_exception_type                                                                         AS exception_type,
    cityHash64(status_message)                                                                 AS status_message_hash,
    multiIf(
        toUInt16OrZero(response_status_code) BETWEEN 400 AND 499, '4xx',
        toUInt16OrZero(response_status_code) >= 500, '5xx',
        has_error, 'err',
        'other'
    )                                                                                          AS http_status_bucket,
    sumState(toUInt64(1))                                                                      AS error_count,
    anyState(trace_id)                                                                         AS sample_trace_id,
    anyState(status_message)                                                                   AS sample_status_message,
    minState(timestamp)                                                                        AS first_seen,
    maxState(timestamp)                                                                        AS last_seen
FROM observability.spans_v2
WHERE has_error = true OR toUInt16OrZero(response_status_code) >= 400
GROUP BY team_id, bucket_ts, service_name, operation_name, exception_type, status_message_hash, http_status_bucket;

-- 2c. Repoint spans_host_to_rollup_1m at spans_v2.
DROP TABLE IF EXISTS observability.spans_host_to_rollup_1m;

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
FROM observability.spans_v2
WHERE mat_host_name != '' OR mat_k8s_pod_name != ''
GROUP BY team_id, bucket_ts, host_name, pod_name, service_name;

-- 2d. Repoint spans_by_version_to_1m at spans_v2.
DROP TABLE IF EXISTS observability.spans_by_version_to_1m;

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
FROM observability.spans_v2
WHERE mat_service_version != ''
  AND (parent_span_id = '' OR parent_span_id = '0000000000000000')
GROUP BY team_id, bucket_ts, service_name, service_version, environment;

-- 2e. Repoint spans_peer_to_rollup_1m at spans_v2.
DROP TABLE IF EXISTS observability.spans_peer_to_rollup_1m;

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
FROM observability.spans_v2
WHERE kind = 3
GROUP BY team_id, bucket_ts, service_name, peer_service, host_name, http_status_bucket;

-- 2f. Repoint spans_kind_to_rollup_1m at spans_v2.
DROP TABLE IF EXISTS observability.spans_kind_to_rollup_1m;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_kind_to_rollup_1m
TO observability.spans_kind_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    service_name                                                                           AS service_name,
    kind_string                                                                            AS kind_string,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1000000.0, toUInt64(1)) AS latency_ms_digest,
    sumState(toUInt64(1))                                                                  AS request_count,
    sumState(duration_nano / 1000000.0)                                                    AS duration_ms_sum
FROM observability.spans_v2
GROUP BY team_id, bucket_ts, service_name, kind_string;

-- 3. Drop the dual-write MV, then the raw legacy spans table.
DROP TABLE IF EXISTS observability.spans_to_spans_v2;

DROP TABLE IF EXISTS observability.spans;
