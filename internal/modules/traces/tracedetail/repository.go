package tracedetail

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// Repository runs trace-id-scoped point lookups for the trace-detail page
// plus the related-traces window query. Queries only — span-event parsing,
// db-statement normalization, and result merging live in service.go.
type Repository interface {
	GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventCombinedRow, error)
	GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error)
	GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
	GetSpanLogs(ctx context.Context, teamID int64, traceID, spanID string) ([]traceLogRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSpanLogs(ctx context.Context, teamID int64, traceID, spanID string) ([]traceLogRow, error) {
	const query = `
		SELECT timestamp, observed_timestamp, severity_text, severity_number,
		       body, trace_id, span_id, trace_flags,
		       service, host, pod, container, environment,
		       attributes_string, attributes_number, attributes_bool,
		       scope_name, scope_version
		FROM observability.logs
		PREWHERE team_id = @teamID AND span_id = @spanID
		WHERE ` + traceIDMatchPredicate + `
		ORDER BY timestamp ASC
		LIMIT 500`
	args := append(traceIDArgs(teamID, traceID), clickhouse.Named("spanID", spanID))
	var rows []traceLogRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "tracedetail.GetSpanLogs", &rows, query, args...)
}

func (r *ClickHouseRepository) GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error) {
	const query = `
		SELECT span_id, trace_id, name AS operation_name, service,
		       CAST(attributes, 'Map(String, String)') AS attributes_string,
		       CAST(map(), 'Map(String, String)')      AS resource_attributes,
		       exception_type, exception_message, exception_stacktrace,
		       db_system, db_name, db_statement,
		       links AS links
		FROM observability.spans
		PREWHERE team_id = @teamID AND span_id = @spanID
		WHERE ` + traceIDMatchPredicate + `
		LIMIT 1`
	args := append(traceIDArgs(teamID, traceID), clickhouse.Named("spanID", spanID))
	var rows []spanAttributeRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "tracedetail.GetSpanAttributes", &rows, query, args...); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	row := rows[0]
	return &row, nil
}

// GetRelatedTraces is window+service+operation scoped (not trace-id-scoped),
// so it benefits from the apm spans_resource CTE — the resource-side scan
// narrows fingerprints to those reporting under @serviceName before the
// raw spans scan.
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
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "tracedetail.GetRelatedTraces", &rows, query, args...)
}

func (r *ClickHouseRepository) GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventCombinedRow, error) {
	const query = `
		SELECT span_id, trace_id, timestamp, events,
		       exception_type, exception_message, exception_stacktrace
		FROM observability.spans
		PREWHERE team_id = @teamID
		WHERE ` + traceIDMatchPredicate + `
		  AND (NOT empty(events) OR NOT empty(exception_type))`
	var rows []spanEventCombinedRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "tracedetail.GetSpanEvents", &rows, query, traceIDArgs(teamID, traceID)...)
}

// traceIDMatchPredicate normalizes trace_id across the empty string + 32-char
// all-zero hex sentinel forms OTLP allows, plus hex casing. Inlined per the
// project rule that retired the shared traces/shared/traceidmatch helper.
const traceIDMatchPredicate = `(
		lowerUTF8(trace_id) = lowerUTF8(@traceID)
		OR (length(@traceID) = 0 AND trace_id = '00000000000000000000000000000000')
		OR (lowerUTF8(@traceID) = '00000000000000000000000000000000' AND length(trace_id) = 0)
	)`

func traceIDArgs(teamID int64, traceID string) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
	}
}

// spanBucketBounds returns the 5-minute-aligned [bucketStart, bucketEnd)
// covering [startMs, endMs] in spans_resource / spans PK terms.
func spanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
