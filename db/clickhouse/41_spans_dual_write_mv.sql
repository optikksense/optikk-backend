-- spans_dual_write — materialized view that mirrors observability.spans into
-- observability.spans_v2 during the cutover window so the v2 schema catches
-- up with live traffic while the ingest pipeline is migrated. The MV is
-- dropped by 42_drop_legacy_spans_objects.sql once the new writer is the
-- source of truth. One-shot backfill of the 30d window is performed by
-- operators at deploy time (see plan §B.3):
--   INSERT INTO observability.spans_v2 SELECT ... FROM observability.spans
--   WHERE timestamp > now() - INTERVAL 30 DAY;
-- Only base columns are copied; MATERIALIZED / ALIAS columns are recomputed
-- by spans_v2 itself.

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_to_spans_v2
TO observability.spans_v2 AS
SELECT
    ts_bucket_start,
    team_id,
    timestamp,
    trace_id,
    span_id,
    parent_span_id,
    trace_state,
    flags,
    name,
    kind,
    kind_string,
    duration_nano,
    has_error,
    is_remote,
    status_code,
    status_code_string,
    status_message,
    http_url,
    http_method,
    http_host,
    external_http_url,
    external_http_method,
    response_status_code,
    attributes,
    events,
    links,
    exception_type,
    exception_message,
    exception_stacktrace,
    exception_escaped
FROM observability.spans;
