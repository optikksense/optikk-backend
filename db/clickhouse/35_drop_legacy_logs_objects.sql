-- Legacy logs pipeline cutover — drop the old rollup cascade, the dual-write
-- MV, and the raw `logs` table after the explorer has flipped to logs_v2 and
-- the backfill has completed. Mirrors the pattern in
-- 26_drop_legacy_metrics_gauges_v2_objects.sql / 29_drop_ai_spans_rollup_objects.sql.
-- MVs first (they depend on the tables), then the rollup tables, then the
-- dual-write MV, then the raw table itself.

DROP TABLE IF EXISTS observability.logs_rollup_5m_to_1h;
DROP TABLE IF EXISTS observability.logs_rollup_1m_to_5m;
DROP TABLE IF EXISTS observability.logs_to_rollup_1m;

DROP TABLE IF EXISTS observability.logs_rollup_1h;
DROP TABLE IF EXISTS observability.logs_rollup_5m;
DROP TABLE IF EXISTS observability.logs_rollup_1m;

DROP TABLE IF EXISTS observability.logs_to_logs_v2;

DROP TABLE IF EXISTS observability.logs;
