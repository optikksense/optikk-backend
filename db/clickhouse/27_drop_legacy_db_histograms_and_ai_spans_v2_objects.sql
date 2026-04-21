-- Pre-merge db histogram rollup used `db_histograms_rollup_v2_*` names before they were merged into
-- `db_histograms_rollup_*` (see 19_rollup_db_histograms.sql). Stale materialized views may still
-- reference the dropped v2 tables; INSERT INTO observability.metrics then fails at prepare time.

DROP TABLE IF EXISTS observability.db_histograms_rollup_v2_5m_to_1h;
DROP TABLE IF EXISTS observability.db_histograms_rollup_v2_1m_to_5m;
DROP TABLE IF EXISTS observability.db_histograms_to_rollup_v2_1m;

DROP TABLE IF EXISTS observability.db_histograms_rollup_v2_1h;
DROP TABLE IF EXISTS observability.db_histograms_rollup_v2_5m;
DROP TABLE IF EXISTS observability.db_histograms_rollup_v2_1m;
