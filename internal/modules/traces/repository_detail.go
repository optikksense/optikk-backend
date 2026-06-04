package traces

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

<<<<<<< HEAD:internal/modules/traces/repository_detail.go
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
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
	)
=======
type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326:internal/modules/traces/detail/repository.go
}

func (r *Repository) GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventCombinedRow, error) {
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
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
	)
}

func (r *Repository) GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error) {
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
		clickhouse.Named("teamID", uint32(teamID)),
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

func (r *Repository) GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
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
		clickhouse.Named("teamID", uint32(teamID)),
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

<<<<<<< HEAD:internal/modules/traces/repository_detail.go
func (r *ClickHouseRepository) GetTraceSummary(ctx context.Context, teamID int64, traceID string) (*TraceSummary, error) {
=======
func (r *Repository) GetTraceSummary(ctx context.Context, teamID int64, traceID string) (*TraceSummary, error) {
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326:internal/modules/traces/detail/repository.go
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
	var rows []struct {
		TraceID        string    `ch:"trace_id"`
		StartTime      time.Time `ch:"start_time"`
		EndTime        time.Time `ch:"end_time"`
		DurationNs     uint64    `ch:"duration_ns"`
		RootService    string    `ch:"root_service"`
		RootOperation  string    `ch:"root_operation"`
		RootStatus     string    `ch:"root_status"`
		RootHTTPMethod string    `ch:"root_http_method"`
		RootHTTPStatus string    `ch:"root_http_status"`
		SpanCount      uint8     `ch:"span_count"`
		HasError       bool      `ch:"has_error"`
		ErrorCount     uint8     `ch:"error_count"`
		ServiceSet     []string  `ch:"service_set"`
		Truncated      bool      `ch:"truncated"`
	}
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "detail.GetTraceSummary", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
	); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	res := rows[0]
	return &TraceSummary{
		TraceID:        res.TraceID,
		StartMs:        uint64(res.StartTime.UnixMilli()),
		EndMs:          uint64(res.EndTime.UnixMilli()),
		DurationMs:     float64(res.DurationNs) / 1_000_000,
		RootService:    res.RootService,
		RootOperation:  res.RootOperation,
		RootStatus:     res.RootStatus,
		RootHTTPMethod: res.RootHTTPMethod,
		RootHTTPStatus: res.RootHTTPStatus,
		SpanCount:      uint32(res.SpanCount),
		HasError:       res.HasError,
		ErrorCount:     uint32(res.ErrorCount),
		ServiceSet:     res.ServiceSet,
		Truncated:      res.Truncated,
	}, nil
}

<<<<<<< HEAD:internal/modules/traces/repository_detail.go
func (r *ClickHouseRepository) ListSpansByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error) {
=======
func (r *Repository) ListSpansByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error) {
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326:internal/modules/traces/detail/repository.go
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
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
	)
}

func spanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
