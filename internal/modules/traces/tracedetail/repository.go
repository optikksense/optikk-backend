package tracedetail

// All methods in this repository intentionally read raw `observability.spans`
// (or `observability.logs` for trace-correlated logs) rather than a rollup.
// The tracedetail page is per-trace drill-down — every query is bounded by
// `trace_id = @tid` (hitting the `idx_trace_id` bloom filter, GRAN 4) or by
// `service_name + name` (hitting `idx_service_name` + `idx_span_name`). With
// those indexes a single drill-in touches at most a few hundred rows.
// Rollups aggregate per-span fields (span_id, parent_span_id, status,
// attributes, body) away, so they cannot serve this page.
//
// If rollup migration becomes attractive later, the only candidates are:
//
//   - GetRelatedTraces: returns per-root-span rows (trace_id, span_id,
//     duration_ms, status, timestamp). The DTO shape forces raw reads — the
//     spans rollup collapses (service_name, operation_name) ↦ percentile
//     state, losing the per-span identifiers. Stays raw.
//   - GetSpanKindBreakdown: group by (trace_id, kind_string) with
//     sum(duration). Trace-scoped; fits a per-trace map-reduce, not a rollup.
//
// See [docs/hld/ingest/ingest.md](../../../../../docs/hld/ingest/ingest.md)
// "Phase 6 — what stays raw" for the full catalog.

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/traceidmatch"
)

var reNumberLiteral = regexp.MustCompile(`\b\d+(\.\d+)?\b`)
var reStringLiteral = regexp.MustCompile(`'[^']*'`)
var reMultiSpace = regexp.MustCompile(`\s+`)

func normalizeDBStatement(stmt string) string {
	if stmt == "" {
		return ""
	}
	s := reStringLiteral.ReplaceAllString(stmt, "?")
	s = reNumberLiteral.ReplaceAllString(s, "?")
	s = reMultiSpace.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

type Repository interface {
	GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventRow, []exceptionRow, error)
	GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error)
	GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
	GetTraceLogs(ctx context.Context, teamID int64, traceID string) ([]traceLogRow, error)
	GetSpanLogs(ctx context.Context, teamID int64, traceID, spanID string) ([]traceLogRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

const (
	tableSpans = "observability.spans"
	tableLogs  = "observability.logs"
)

// GetTraceLogs returns the logs associated with a particular trace.
func (r *ClickHouseRepository) GetTraceLogs(ctx context.Context, teamID int64, traceID string) ([]traceLogRow, error) {
	var rows []traceLogRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, `
		SELECT timestamp, observed_timestamp, severity_text, severity_number,
			body, trace_id, span_id, trace_flags,
			service, host, pod, container, environment,
			attributes_string, attributes_number, attributes_bool,
			scope_name, scope_version
		FROM observability.logs
		WHERE team_id = @teamID AND `+traceidmatch.WhereTraceIDMatchesCH("trace_id", "traceID")+`
		ORDER BY timestamp ASC
		LIMIT 1000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)); err != nil { //nolint:gosec // G115
		return nil, err
	}
	return rows, nil
}

// GetSpanLogs returns the logs for a specific span within a trace. Used by the
// Logs tab in the span detail drawer (O8).
func (r *ClickHouseRepository) GetSpanLogs(ctx context.Context, teamID int64, traceID, spanID string) ([]traceLogRow, error) {
	var rows []traceLogRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, `
		SELECT timestamp, observed_timestamp, severity_text, severity_number,
			body, trace_id, span_id, trace_flags,
			service, host, pod, container, environment,
			attributes_string, attributes_number, attributes_bool,
			scope_name, scope_version
		FROM observability.logs
		WHERE team_id = @teamID AND `+traceidmatch.WhereTraceIDMatchesCH("trace_id", "traceID")+`
		  AND span_id = @spanID
		ORDER BY timestamp ASC
		LIMIT 500
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID), clickhouse.Named("spanID", spanID)); err != nil { //nolint:gosec // G115
		return nil, err
	}
	return rows, nil
}

// GetSpanAttributes returns all attributes for a given span. Also returns the
// serialized OTLP span `links` string so the drawer can render linked traces (O13).
func (r *ClickHouseRepository) GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error) {
	var rows []spanAttributeRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, `
		SELECT s.span_id, s.trace_id, s.name AS operation_name, s.service_name,
		       CAST(s.attributes, 'Map(String, String)') AS attributes_string,
		       CAST(map(), 'Map(String, String)') AS resource_attributes,
		       s.exception_type, s.exception_message, s.exception_stacktrace,
		       s.mat_db_system AS db_system, s.mat_db_name AS db_name, s.mat_db_statement AS db_statement,
		       s.links AS links
		FROM observability.spans s
		WHERE s.team_id = @teamID AND `+traceidmatch.WhereTraceIDMatchesCH("s.trace_id", "traceID")+` AND s.span_id = @spanID
		LIMIT 1
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID), clickhouse.Named("spanID", spanID)); err != nil { //nolint:gosec // G115
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	row := rows[0]
	return &row, nil
}

// GetRelatedTraces returns other traces from the same service and operation.
func (r *ClickHouseRepository) GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	var rows []RelatedTrace
	err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, `
		SELECT s.span_id, s.trace_id, s.name AS operation_name, s.service_name,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.status_code_string AS status, s.timestamp AS start_time
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+rootspan.Condition("s")+`
		  AND s.service_name = @serviceName
		  AND s.name = @operationName
		  AND s.trace_id != @excludeTraceID
		ORDER BY s.timestamp DESC
		LIMIT @limit
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("operationName", operationName),
		clickhouse.Named("excludeTraceID", excludeTraceID),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

// GetSpanEvents returns span events and exceptions in a single scan.
func (r *ClickHouseRepository) GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventRow, []exceptionRow, error) {
	var rows []spanEventCombinedRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, `
		SELECT span_id, trace_id, timestamp, events,
		       exception_type, exception_message, exception_stacktrace
		FROM observability.spans
		PREWHERE team_id = @teamID
		WHERE `+traceidmatch.WhereTraceIDMatchesCH("trace_id", "traceID")+`
		  AND (NOT empty(events) OR NOT empty(exception_type))
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)); err != nil { //nolint:gosec // G115
		return nil, nil, err
	}
	return splitSpanEventRows(rows)
}

func splitSpanEventRows(rows []spanEventCombinedRow) ([]spanEventRow, []exceptionRow, error) {
	var events []spanEventRow
	var exceptions []exceptionRow
	for _, r := range rows {
		for _, ev := range r.Events {
			events = append(events, spanEventRow{
				SpanID: r.SpanID, TraceID: r.TraceID, Timestamp: r.Timestamp, EventJSON: ev,
			})
		}
		if r.ExceptionType != "" {
			exceptions = append(exceptions, exceptionRow{
				SpanID: r.SpanID, TraceID: r.TraceID, Timestamp: r.Timestamp,
				ExceptionType: r.ExceptionType, ExceptionMessage: r.ExceptionMessage,
				ExceptionStacktrace: r.ExceptionStacktrace,
			})
		}
	}
	// Reverse exceptions for display order (latest first)
	for i, j := 0, len(exceptions)-1; i < j; i, j = i+1, j-1 {
		exceptions[i], exceptions[j] = exceptions[j], exceptions[i]
	}
	return events, exceptions, nil
}

