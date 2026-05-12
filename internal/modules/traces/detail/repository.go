package detail

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

type Repository interface {
	GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventCombinedRow, error)
	GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error)
	GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
	GetTraceSummary(ctx context.Context, teamID int64, traceID string) (*traceSummaryRow, error)
	ListSpansByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error)
	ListSpanSubtree(ctx context.Context, teamID int64, spanID string) ([]SpanListItem, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error) {
	const query = `
		WITH trace_loc AS (
		    SELECT ts_bucket, fingerprint
		    FROM observability.trace_index
		    PREWHERE trace_id = @traceID AND team_id = @teamID
		)
		SELECT span_id, trace_id, name AS operation_name, service,
		       toJSONString(attributes)                AS attributes_json,
		       exception_type,
			   exception_message, 
			   exception_stacktrace,
		       db_system, 
			   db_name, 
			   db_statement,
		       links AS links
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND (ts_bucket, fingerprint) IN (SELECT ts_bucket, fingerprint FROM trace_loc)
		     AND span_id  = @spanID
		     AND trace_id = @traceID
		LIMIT 1`
	var rows []spanAttributeRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "detail.GetSpanAttributes", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
		clickhouse.Named("spanID", spanID),
	); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	row := rows[0]
	return &row, nil
}

func (r *ClickHouseRepository) GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service  = @serviceName
		)
		SELECT span_id,
		       trace_id,
		       name                       AS operation_name,
		       service,
		       duration_nano / 1000000.0  AS duration_ms,
		       status_code_string         AS status,
		       timestamp                  AS start_time
		FROM observability.spans
		PREWHERE team_id      = @teamID
		     AND ts_bucket    BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint  IN active_fps
		     AND service      = @serviceName
		     AND name         = @operationName
		WHERE is_root = 1
		  AND timestamp BETWEEN @start AND @end
		  AND trace_id != @excludeTraceID
		ORDER BY timestamp DESC
		LIMIT @limit`
	bucketStart, bucketEnd := spanBucketBounds(startMs, endMs)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("operationName", operationName),
		clickhouse.Named("excludeTraceID", excludeTraceID),
		clickhouse.Named("limit", limit),
	}
	var rows []RelatedTrace
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "detail.GetRelatedTraces", &rows, query, args...)
}

func (r *ClickHouseRepository) GetTraceSummary(ctx context.Context, teamID int64, traceID string) (*traceSummaryRow, error) {
	const query = `
		WITH trace_loc AS (
		    SELECT ts_bucket, fingerprint
		    FROM observability.trace_index
		    PREWHERE trace_id = @traceID AND team_id = @teamID
		)
		SELECT trace_id,
		       timestamp                              AS start_time,
		       timestamp                              AS end_time,
		       duration_nano                          AS duration_ns,
		       service                                AS root_service,
		       name                                   AS root_operation,
		       status_code_string                     AS root_status,
		       http_method                            AS root_http_method,
		       response_status_code                   AS root_http_status,
		       1                                      AS span_count,
		       has_error,
		       (CASE WHEN has_error THEN 1 ELSE 0 END) AS error_count,
		       [service]                              AS service_set,
		       false                                  AS truncated
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND (ts_bucket, fingerprint) IN (SELECT ts_bucket, fingerprint FROM trace_loc)
		     AND trace_id = @traceID
		     AND is_root  = 1
		ORDER BY timestamp DESC
		LIMIT 1`
	var rows []traceSummaryRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "detail.GetTraceSummary", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
	); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

func (r *ClickHouseRepository) GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventCombinedRow, error) {
	const query = `
		WITH trace_loc AS (
		    SELECT ts_bucket, fingerprint
		    FROM observability.trace_index
		    PREWHERE trace_id = @traceID AND team_id = @teamID
		)
		SELECT span_id, trace_id, timestamp, events,
		       exception_type, exception_message, exception_stacktrace
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND (ts_bucket, fingerprint) IN (SELECT ts_bucket, fingerprint FROM trace_loc)
		     AND trace_id = @traceID
		WHERE NOT empty(events) OR NOT empty(exception_type)`
	var rows []spanEventCombinedRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "detail.GetSpanEvents", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
	)
}

func (r *ClickHouseRepository) ListSpansByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error) {
	const query = `
		WITH trace_loc AS (
		    SELECT ts_bucket, fingerprint
		    FROM observability.trace_index
		    PREWHERE trace_id = @traceID AND team_id = @teamID
		)
		SELECT span_id,
		       parent_span_id,
		       trace_id,
		       service,
		       name,
		       kind_string,
		       status_code_string,
		       has_error,
		       duration_nano / 1000000.0          AS duration_ms,
		       timestamp
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND (ts_bucket, fingerprint) IN (SELECT ts_bucket, fingerprint FROM trace_loc)
		     AND trace_id = @traceID
		ORDER BY timestamp ASC
		LIMIT 5000`
	var rows []SpanListItem
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "detail.ListSpansByTrace", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
	)
}

func (r *ClickHouseRepository) ListSpanSubtree(ctx context.Context, teamID int64, spanID string) ([]SpanListItem, error) {
	const query = `
		WITH start AS (
		    SELECT trace_id
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND span_id = @spanID
		    LIMIT 1
		)
		SELECT span_id,
		       parent_span_id,
		       trace_id,
		       service,
		       name,
		       kind_string,
		       status_code_string,
		       has_error,
		       duration_nano / 1000000.0          AS duration_ms,
		       timestamp
		FROM observability.spans
		PREWHERE team_id = @teamID
		WHERE trace_id IN (SELECT trace_id FROM start)
		ORDER BY timestamp ASC
		LIMIT 5000`
	var rows []SpanListItem
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "detail.ListSpanSubtree", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("spanID", spanID),
	)
}

func spanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
