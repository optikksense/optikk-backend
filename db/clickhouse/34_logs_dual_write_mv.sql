-- logs_dual_write — materialized view that mirrors observability.logs into
-- observability.logs_v2 during the cutover window so the v2 schema catches
-- up with live traffic while the ingest pipeline is migrated. The MV is
-- dropped by 35_drop_legacy_logs_objects.sql once the new writer is the
-- source of truth. One-shot backfill of the 30d window is performed by
-- operators at deploy time (see plan §A.2):
--   INSERT INTO observability.logs_v2 SELECT ... FROM observability.logs
--   WHERE timestamp > now() - INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_to_logs_v2
TO observability.logs_v2 AS
SELECT
    team_id,
    ts_bucket_start,
    timestamp,
    observed_timestamp,
    trace_id,
    span_id,
    trace_flags,
    severity_text,
    severity_number,
    body,
    attributes_string,
    attributes_number,
    attributes_bool,
    resource,
    resource_fingerprint,
    scope_name,
    scope_version
FROM observability.logs;
