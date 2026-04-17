package tracedetail

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
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

func isRootParentSpanID(parentID string) bool {
	trimmed := strings.Trim(parentID, "\x00")
	return trimmed == "" || trimmed == "0000000000000000"
}

type Repository interface {
	GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventRow, []exceptionRow, error)
	GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]spanKindDurationRow, error)
	GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]criticalPathRow, error)
	GetSpanSelfTimes(ctx context.Context, teamID int64, traceID string) ([]SpanSelfTime, error)
	GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]errorPathRow, error)
	GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error)
	GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
	GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]flamegraphRow, error)
	GetTraceLogs(ctx context.Context, teamID int64, traceID string) ([]traceLogRow, error)

}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

const (
	tableSpans = "observability.spans"
)

// GetTraceLogs returns the logs associated with a particular trace.
func (r *ClickHouseRepository) GetTraceLogs(ctx context.Context, teamID int64, traceID string) ([]traceLogRow, error) {
	var rows []traceLogRow
	if err := r.db.SelectExplorer(ctx, &rows, `
		SELECT timestamp, observed_timestamp, severity_text, severity_number,
			body, trace_id, span_id, trace_flags,
			service, host, pod, container, environment,
			attributes_string, attributes_number, attributes_bool,
			scope_name, scope_version
		FROM observability.logs
		WHERE team_id = @teamID AND trace_id = @traceID
		ORDER BY timestamp ASC
		LIMIT 1000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)); err != nil { //nolint:gosec // G115
		return nil, err
	}
	return rows, nil
}

// GetSpanKindBreakdown returns a breakdown of traces by their span kind.
func (r *ClickHouseRepository) GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]spanKindDurationRow, error) {
	var rows []spanKindDurationRow
	err := r.db.SelectExplorer(ctx, &rows, `
		SELECT kind_string                        AS span_kind,
		       sum(duration_nano) / 1000000.0     AS total_duration_ms,
		       toInt64(count())                   AS span_count
		FROM observability.spans
		WHERE team_id = @teamID AND trace_id = @traceID
		GROUP BY kind_string
		ORDER BY total_duration_ms DESC
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

// GetCriticalPath finds the critical path in the trace.
func (r *ClickHouseRepository) GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]criticalPathRow, error) {
	var rows []criticalPathRow
	err := r.db.SelectExplorer(ctx, &rows, `
		SELECT s.span_id, s.parent_span_id,
		       s.name AS operation_name,
		       s.service_name,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       toUnixTimestamp64Nano(s.timestamp) AS start_ns,
		       toUnixTimestamp64Nano(s.timestamp) + s.duration_nano AS end_ns
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID
		ORDER BY start_ns ASC
		LIMIT 5000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

// GetSpanAttributes returns all attributes for a given span.
func (r *ClickHouseRepository) GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error) {
	var rows []spanAttributeRow
	if err := r.db.SelectExplorer(ctx, &rows, `
		SELECT s.span_id, s.trace_id, s.name AS operation_name, s.service_name,
		       CAST(s.attributes, 'Map(String, String)') AS attributes_string,
		       CAST(map(), 'Map(String, String)') AS resource_attributes,
		       s.exception_type, s.exception_message, s.exception_stacktrace,
		       s.mat_db_system AS db_system, s.mat_db_name AS db_name, s.mat_db_statement AS db_statement
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID AND s.span_id = @spanID
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
	err := r.db.SelectExplorer(ctx, &rows, `
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

// GetSpanEvents returns span specific events like exceptions.
func (r *ClickHouseRepository) GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventRow, []exceptionRow, error) {
	var eventRows []spanEventRow
	if err := r.db.SelectExplorer(ctx, &eventRows, `
		SELECT span_id, trace_id, timestamp, arrayJoin(events) AS event_json
		FROM observability.spans
		WHERE team_id = @teamID AND trace_id = @traceID
		  AND NOT empty(events)
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)); err != nil { //nolint:gosec // G115
		return nil, nil, err
	}

	var exceptionRows []exceptionRow
	if err := r.db.SelectExplorer(ctx, &exceptionRows, `
		SELECT span_id, trace_id, timestamp,
		       exception_type, exception_message, exception_stacktrace
		FROM observability.spans
		WHERE team_id = @teamID AND trace_id = @traceID
		  AND NOT empty(exception_type)
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)); err != nil { //nolint:gosec // G115
		return nil, nil, err
	}

	for i, j := 0, len(exceptionRows)-1; i < j; i, j = i+1, j-1 {
		exceptionRows[i], exceptionRows[j] = exceptionRows[j], exceptionRows[i]
	}
	return eventRows, exceptionRows, nil
}

// GetFlamegraphData returns the raw spans for the flamegraph visualization.
func (r *ClickHouseRepository) GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]flamegraphRow, error) {
	var rows []flamegraphRow
	err := r.db.SelectExplorer(ctx, &rows, `
		SELECT span_id, parent_span_id, name AS operation_name, service_name,
		       kind_string AS span_kind, duration_nano / 1000000.0 AS duration_ms,
		       toUnixTimestamp64Nano(timestamp) AS start_ns, has_error
		FROM observability.spans
		WHERE team_id = @teamID AND trace_id = @traceID
		ORDER BY start_ns ASC
		LIMIT 10000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

// GetSpanSelfTimes returns the duration for each span.
func (r *ClickHouseRepository) GetSpanSelfTimes(ctx context.Context, teamID int64, traceID string) ([]SpanSelfTime, error) {
	var rows []SpanSelfTime
	err := r.db.SelectExplorer(ctx, &rows, `
		SELECT s.span_id, s.name AS operation_name,
		       s.duration_nano / 1000000.0 AS total_duration_ms,
		       (s.duration_nano - coalesce(cs.child_duration, 0)) / 1000000.0 AS self_time_ms,
		       coalesce(cs.child_duration, 0) / 1000000.0 AS child_time_ms
		FROM observability.spans s
		LEFT JOIN (
			SELECT parent_span_id, sum(duration_nano) AS child_duration
			FROM observability.spans
			WHERE team_id = @teamID AND trace_id = @traceID
			GROUP BY parent_span_id
		) cs ON s.span_id = cs.parent_span_id
		WHERE s.team_id = @teamID AND s.trace_id = @traceID
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

// GetErrorPath returns the path of spans that caused the error.
func (r *ClickHouseRepository) GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]errorPathRow, error) {
	var rows []errorPathRow
	err := r.db.SelectExplorer(ctx, &rows, `
		SELECT s.span_id, s.parent_span_id, s.name AS operation_name,
		       s.service_name AS service_name, s.status_code_string AS status, s.status_message,
		       s.timestamp AS start_time, s.duration_nano / 1000000.0 AS duration_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID
		  AND (s.has_error = true OR s.status_code_string = 'ERROR')
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}
