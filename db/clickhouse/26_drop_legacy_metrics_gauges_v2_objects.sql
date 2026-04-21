-- Legacy gauge rollup used metrics_gauges_rollup_v2_* table names before v1/v2 were merged into
-- metrics_gauges_rollup_* (see 17_rollup_metrics_gauges.sql). Old materialized views may still
-- reference the dropped v2 tables; INSERT INTO observability.metrics then fails at prepare time
-- with "Target table ... metrics_gauges_rollup_v2_1m ... doesn't exist".
-- Drop MVs first (they depend on the rollup tables), then drop any leftover v2 rollup tables.

DROP TABLE IF EXISTS observability.metrics_gauges_rollup_v2_5m_to_1h;
DROP TABLE IF EXISTS observability.metrics_gauges_rollup_v2_1m_to_5m;
DROP TABLE IF EXISTS observability.metrics_gauges_to_rollup_v2_1m;

DROP TABLE IF EXISTS observability.metrics_gauges_rollup_v2_1h;
DROP TABLE IF EXISTS observability.metrics_gauges_rollup_v2_5m;
DROP TABLE IF EXISTS observability.metrics_gauges_rollup_v2_1m;
