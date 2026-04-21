-- GenAI / `ai_spans_*` rollup pipeline is not used. Drop all related MVs and tables so inserts into
-- `observability.spans` are not routed through materialized views targeting missing `ai_spans_rollup_*`.
-- Raw spans (including gen_ai attributes on the JSON column) remain in `observability.spans`.

DROP TABLE IF EXISTS observability.ai_spans_rollup_5m_to_1h;
DROP TABLE IF EXISTS observability.ai_spans_rollup_1m_to_5m;
DROP TABLE IF EXISTS observability.ai_spans_to_rollup_1m;

DROP TABLE IF EXISTS observability.ai_spans_rollup_1h;
DROP TABLE IF EXISTS observability.ai_spans_rollup_5m;
DROP TABLE IF EXISTS observability.ai_spans_rollup_1m;

DROP TABLE IF EXISTS observability.ai_spans_rollup_v2_5m_to_1h;
DROP TABLE IF EXISTS observability.ai_spans_rollup_v2_1m_to_5m;
DROP TABLE IF EXISTS observability.ai_spans_to_rollup_v2_1m;

DROP TABLE IF EXISTS observability.ai_spans_rollup_v2_1h;
DROP TABLE IF EXISTS observability.ai_spans_rollup_v2_5m;
DROP TABLE IF EXISTS observability.ai_spans_rollup_v2_1m;
