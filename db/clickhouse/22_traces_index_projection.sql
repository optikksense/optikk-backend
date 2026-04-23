-- Phase 7 perf: projection on traces_index sorted by (team_id, start_ms DESC, trace_id DESC)
-- so POST /traces/query list-path can keyset-scan the latest 50 traces
-- without reading the whole ts_bucket_start window. The table's primary
-- ORDER BY (team_id, ts_bucket_start, root_service, start_ms, trace_id) puts
-- root_service between team_id and start_ms, so list-by-time reads scan wide.

ALTER TABLE observability.traces_index
    ADD PROJECTION IF NOT EXISTS by_start_desc (
        SELECT *
        ORDER BY team_id, start_ms DESC, trace_id DESC
    );

-- Materialise against existing parts. New parts get the projection automatically.
-- Idempotent: ALTER TABLE … MATERIALIZE PROJECTION is a no-op when already built.
ALTER TABLE observability.traces_index
    MATERIALIZE PROJECTION by_start_desc;
