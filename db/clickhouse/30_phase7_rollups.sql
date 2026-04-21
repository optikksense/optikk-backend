
-- =============================================================================
-- Phase 7 — finish-the-job rollups: gauges, DB, messaging, topology
-- =============================================================================
-- Closes the remaining raw-scan aggregate gaps: infrastructure gauges
-- (cpu/mem/disk/network/jvm/k8s/connpool), apm process metrics + httpmetrics
-- gauges, saturation DB per-domain histogram breakdowns, saturation Kafka
-- messaging, and service-to-service topology edges. Each base table gets _5m
-- and _1h cascade tiers.

-- ---------------------------------------------------------------------------
-- A1. metrics_gauges_rollup — generic gauge + counter metric rollup.
--     Covers infrastructure/{cpu,disk,memory,network,jvm,kubernetes,connpool}
--     + overview/apm (Uptime/OpenFDs/ProcessCPU/ProcessMemory/ActiveRequests)
--     + overview/httpmetrics (ActiveRequests).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_rollup_1m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    service        LowCardinality(String),
    host           LowCardinality(String),
    pod            LowCardinality(String),
    state_dim      LowCardinality(String),

    value_sum      AggregateFunction(sum, Float64),
    value_avg_num  AggregateFunction(sum, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    value_max      AggregateFunction(max, Float64),
    value_min      AggregateFunction(min, Float64),
    value_last     AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, host, pod, state_dim)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_to_rollup_1m
TO observability.metrics_gauges_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                   AS bucket_ts,
    metric_name                                                                  AS metric_name,
    service                                                                      AS service,
    host                                                                         AS host,
    attributes.`k8s.pod.name`::String                                            AS pod,
    multiIf(
        metric_name IN ('system.cpu.time','system.cpu.utilization'),
            attributes.`system.cpu.state`::String,
        metric_name = 'process.cpu.time',
            attributes.`process.cpu.state`::String,
        metric_name IN ('system.memory.usage','system.memory.utilization'),
            attributes.`system.memory.state`::String,
        metric_name IN ('system.disk.io','system.disk.operations'),
            attributes.`system.disk.direction`::String,
        metric_name IN ('system.network.io','system.network.packets','system.network.errors','system.network.connections'),
            attributes.`system.network.direction`::String,
        metric_name IN ('jvm.memory.used','jvm.memory.committed','jvm.memory.limit','jvm.memory.used_after_last_gc'),
            attributes.`jvm.memory.pool.name`::String,
        metric_name = 'jvm.gc.duration',
            attributes.`jvm.gc.name`::String,
        ''
    )                                                                            AS state_dim,

    sumState(value)                                                              AS value_sum,
    sumState(value)                                                              AS value_avg_num,
    sumState(toUInt64(1))                                                        AS sample_count,
    maxState(value)                                                              AS value_max,
    minState(value)                                                              AS value_min,
    argMaxState(value, timestamp)                                                AS value_last
FROM observability.metrics
WHERE metric_type IN ('Gauge','Sum') AND hist_count = 0;

-- ---------------------------------------------------------------------------
-- A2. metrics_gauges_by_status_rollup — HTTP request rate by status.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_by_status_rollup_1m (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String),
    service          LowCardinality(String),
    http_status_code LowCardinality(String),
    sample_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, http_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_by_status_to_rollup_1m
TO observability.metrics_gauges_by_status_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                    AS bucket_ts,
    metric_name                                                   AS metric_name,
    service                                                       AS service,
    attributes.`http.status_code`::String                         AS http_status_code,
    sumState(toUInt64(1))                                         AS sample_count
FROM observability.metrics
WHERE metric_name IN ('http.server.request.duration','http.server.request.size','http.client.request.duration');

-- ---------------------------------------------------------------------------
-- A3. db_histograms_rollup — saturation/database histogram-metric rollup
--     keyed by db.* attributes.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_1m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    metric_name     LowCardinality(String),
    service         LowCardinality(String),
    db_system       LowCardinality(String),
    db_operation    LowCardinality(String),
    db_collection   LowCardinality(String),
    db_namespace    LowCardinality(String),
    pool_name       LowCardinality(String),
    error_type      LowCardinality(String),
    server_address  LowCardinality(String),

    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_to_rollup_1m
TO observability.db_histograms_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    metric_name                                                                            AS metric_name,
    service                                                                                AS service,
    attributes.`db.system`::String                                                         AS db_system,
    attributes.`db.operation`::String                                                      AS db_operation,
    attributes.`db.collection.name`::String                                                AS db_collection,
    attributes.`db.namespace`::String                                                      AS db_namespace,
    attributes.`pool.name`::String                                                         AS pool_name,
    attributes.`error.type`::String                                                        AS error_type,
    attributes.`server.address`::String                                                    AS server_address,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        hist_sum / if(hist_count = 0, 1, hist_count),
        hist_count
    )                                                                                      AS latency_ms_digest,
    sumState(hist_count)                                                                   AS hist_count,
    sumState(hist_sum)                                                                     AS hist_sum
FROM observability.metrics
WHERE metric_type = 'Histogram'
  AND hist_count > 0
  AND (metric_name LIKE 'db.%' OR metric_name LIKE 'pool.%');

-- ---------------------------------------------------------------------------
-- A4. messaging_histograms_rollup — saturation/kafka rollup.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.messaging_histograms_rollup_1m (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    messaging_system      LowCardinality(String),
    messaging_destination LowCardinality(String),
    messaging_operation   LowCardinality(String),
    consumer_group        LowCardinality(String),

    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_histograms_to_rollup_1m
TO observability.messaging_histograms_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    metric_name                                                                            AS metric_name,
    service                                                                                AS service,
    attributes.`messaging.system`::String                                                  AS messaging_system,
    attributes.`messaging.destination.name`::String                                        AS messaging_destination,
    attributes.`messaging.operation`::String                                               AS messaging_operation,
    attributes.`messaging.kafka.consumer.group`::String                                    AS consumer_group,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        hist_sum / if(hist_count = 0, 1, hist_count),
        hist_count
    )                                                                                      AS latency_ms_digest,
    sumState(hist_count)                                                                   AS hist_count,
    sumState(hist_sum)                                                                     AS hist_sum
FROM observability.metrics
WHERE metric_type = 'Histogram'
  AND hist_count > 0
  AND metric_name LIKE 'messaging.%';

-- ---------------------------------------------------------------------------
-- A5. spans_topology_rollup — service-to-service edges from CLIENT spans.
--     Uses peer.service attribute (set by OTel SDKs on CLIENT spans) as the
--     edge target; service_name as source. Single-pass, no self-join.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.spans_topology_rollup_1m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    client_service  LowCardinality(String),
    server_service  LowCardinality(String),
    operation       LowCardinality(String),

    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, client_service, server_service, operation)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

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
FROM observability.spans
WHERE kind = 3 AND mat_peer_service != ''; -- SPAN_KIND_CLIENT = 3

-- =============================================================================
-- Phase 7 cascade tiers — _5m + _1h for all 5 new rollups
-- =============================================================================

-- metrics_gauges_rollup 5m/1h
CREATE TABLE IF NOT EXISTS observability.metrics_gauges_rollup_5m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    service        LowCardinality(String),
    host           LowCardinality(String),
    pod            LowCardinality(String),
    state_dim      LowCardinality(String),
    value_sum      AggregateFunction(sum, Float64),
    value_avg_num  AggregateFunction(sum, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    value_max      AggregateFunction(max, Float64),
    value_min      AggregateFunction(min, Float64),
    value_last     AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, host, pod, state_dim)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_rollup_1m_to_5m
TO observability.metrics_gauges_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, host, pod, state_dim,
       sumMergeState(value_sum)       AS value_sum,
       sumMergeState(value_avg_num)   AS value_avg_num,
       sumMergeState(sample_count)    AS sample_count,
       maxMergeState(value_max)       AS value_max,
       minMergeState(value_min)       AS value_min,
       argMaxMergeState(value_last)   AS value_last
FROM observability.metrics_gauges_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, host, pod, state_dim;

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_rollup_1h (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    service        LowCardinality(String),
    host           LowCardinality(String),
    pod            LowCardinality(String),
    state_dim      LowCardinality(String),
    value_sum      AggregateFunction(sum, Float64),
    value_avg_num  AggregateFunction(sum, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    value_max      AggregateFunction(max, Float64),
    value_min      AggregateFunction(min, Float64),
    value_last     AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, host, pod, state_dim)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_rollup_5m_to_1h
TO observability.metrics_gauges_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, host, pod, state_dim,
       sumMergeState(value_sum)       AS value_sum,
       sumMergeState(value_avg_num)   AS value_avg_num,
       sumMergeState(sample_count)    AS sample_count,
       maxMergeState(value_max)       AS value_max,
       minMergeState(value_min)       AS value_min,
       argMaxMergeState(value_last)   AS value_last
FROM observability.metrics_gauges_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, host, pod, state_dim;

-- metrics_gauges_by_status_rollup 5m/1h
CREATE TABLE IF NOT EXISTS observability.metrics_gauges_by_status_rollup_5m (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String),
    service          LowCardinality(String),
    http_status_code LowCardinality(String),
    sample_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, http_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_by_status_rollup_1m_to_5m
TO observability.metrics_gauges_by_status_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, http_status_code,
       sumMergeState(sample_count) AS sample_count
FROM observability.metrics_gauges_by_status_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, http_status_code;

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_by_status_rollup_1h (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String),
    service          LowCardinality(String),
    http_status_code LowCardinality(String),
    sample_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, http_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_by_status_rollup_5m_to_1h
TO observability.metrics_gauges_by_status_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, http_status_code,
       sumMergeState(sample_count) AS sample_count
FROM observability.metrics_gauges_by_status_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, http_status_code;

-- db_histograms_rollup 5m/1h
CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_5m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    metric_name     LowCardinality(String),
    service         LowCardinality(String),
    db_system       LowCardinality(String),
    db_operation    LowCardinality(String),
    db_collection   LowCardinality(String),
    db_namespace    LowCardinality(String),
    pool_name       LowCardinality(String),
    error_type      LowCardinality(String),
    server_address  LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_rollup_1m_to_5m
TO observability.db_histograms_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.db_histograms_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address;

CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_1h (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    metric_name     LowCardinality(String),
    service         LowCardinality(String),
    db_system       LowCardinality(String),
    db_operation    LowCardinality(String),
    db_collection   LowCardinality(String),
    db_namespace    LowCardinality(String),
    pool_name       LowCardinality(String),
    error_type      LowCardinality(String),
    server_address  LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_rollup_5m_to_1h
TO observability.db_histograms_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.db_histograms_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address;

-- messaging_histograms_rollup 5m/1h
CREATE TABLE IF NOT EXISTS observability.messaging_histograms_rollup_5m (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    messaging_system      LowCardinality(String),
    messaging_destination LowCardinality(String),
    messaging_operation   LowCardinality(String),
    consumer_group        LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_histograms_rollup_1m_to_5m
TO observability.messaging_histograms_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.messaging_histograms_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group;

CREATE TABLE IF NOT EXISTS observability.messaging_histograms_rollup_1h (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    messaging_system      LowCardinality(String),
    messaging_destination LowCardinality(String),
    messaging_operation   LowCardinality(String),
    consumer_group        LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_histograms_rollup_5m_to_1h
TO observability.messaging_histograms_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.messaging_histograms_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group;

-- spans_topology_rollup 5m/1h
CREATE TABLE IF NOT EXISTS observability.spans_topology_rollup_5m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    client_service  LowCardinality(String),
    server_service  LowCardinality(String),
    operation       LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, client_service, server_service, operation)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_topology_rollup_1m_to_5m
TO observability.spans_topology_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       client_service, server_service, operation,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count) AS request_count,
       sumMergeState(error_count)   AS error_count
FROM observability.spans_topology_rollup_1m
GROUP BY team_id, bucket_ts, client_service, server_service, operation;

CREATE TABLE IF NOT EXISTS observability.spans_topology_rollup_1h (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    client_service  LowCardinality(String),
    server_service  LowCardinality(String),
    operation       LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, client_service, server_service, operation)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_topology_rollup_5m_to_1h
TO observability.spans_topology_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       client_service, server_service, operation,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count) AS request_count,
       sumMergeState(error_count)   AS error_count
FROM observability.spans_topology_rollup_5m
GROUP BY team_id, bucket_ts, client_service, server_service, operation;

