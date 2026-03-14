package traces

import (
	"context"
	"fmt"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func rawTimeBucketExpr(startMs, endMs int64, column string) string {
	return timebucket.ExprForColumn(startMs, endMs, column)
}

// GetTracesKeyset returns a page of traces using keyset (cursor) pagination.
func (r *ClickHouseRepository) GetTracesKeyset(ctx context.Context, f TraceFilters, limit int, cursor TraceCursor) ([]Trace, TraceSummary, bool, error) {
	queryFrag, args := buildWhereClause(f)

	if !cursor.Timestamp.IsZero() && cursor.SpanID != "" {
		queryFrag += ` AND (s.timestamp < ? OR (s.timestamp = ? AND s.span_id < ?))`
		args = append(args, cursor.Timestamp, cursor.Timestamp, cursor.SpanID)
	}

	selectCols := traceSelectColumns(f.SearchMode)
	query := `SELECT ` + selectCols + ` FROM observability.spans s` + queryFrag + ` ORDER BY s.timestamp DESC, s.span_id DESC LIMIT ?`
	rows, err := dbutil.QueryMaps(r.db, query, append(args, limit+1)...)
	if err != nil {
		return nil, TraceSummary{}, false, err
	}

	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}

	traces := make([]Trace, 0, len(rows))
	for _, row := range rows {
		traces = append(traces, traceFromRow(row))
	}

	summaryFrag, summaryArgs := buildWhereClause(f)
	summaryRow, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as total_traces,
		       sum(if(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400, 1, 0)) as error_traces,
		       AVG(s.duration_nano / 1000000.0) as avg_duration,
		       quantile(0.5)(s.duration_nano / 1000000.0) as p50_duration,
		       quantile(0.95)(s.duration_nano / 1000000.0) as p95_duration,
		       quantile(0.99)(s.duration_nano / 1000000.0) as p99_duration
		FROM observability.spans s`+summaryFrag, summaryArgs...)
	if err != nil {
		return traces, TraceSummary{}, hasMore, nil
	}

	return traces, summaryFromRow(summaryRow), hasMore, nil
}

// GetTraces returns traces with legacy OFFSET-based pagination.
func (r *ClickHouseRepository) GetTraces(ctx context.Context, f TraceFilters, limit, offset int) ([]Trace, int64, TraceSummary, error) {
	queryFrag, args := buildWhereClause(f)

	selectCols := traceSelectColumns(f.SearchMode)
	query := `SELECT ` + selectCols + ` FROM observability.spans s` + queryFrag + ` ORDER BY s.timestamp DESC LIMIT ? OFFSET ?`
	rows, err := dbutil.QueryMaps(r.db, query, append(args, limit, offset)...)
	if err != nil {
		return nil, 0, TraceSummary{}, err
	}

	traces := make([]Trace, 0, len(rows))
	for _, row := range rows {
		traces = append(traces, traceFromRow(row))
	}

	countFrag, countArgs := buildWhereClause(f)
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM observability.spans s`+countFrag, countArgs...)

	summaryFrag, summaryArgs := buildWhereClause(f)
	summaryRow, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as total_traces,
		       sum(if(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400, 1, 0)) as error_traces,
		       AVG(s.duration_nano / 1000000.0) as avg_duration,
		       quantile(0.5)(s.duration_nano / 1000000.0) as p50_duration,
		       quantile(0.95)(s.duration_nano / 1000000.0) as p95_duration,
		       quantile(0.99)(s.duration_nano / 1000000.0) as p99_duration
		FROM observability.spans s`+summaryFrag, summaryArgs...)
	if err != nil {
		return traces, total, TraceSummary{}, err
	}

	return traces, total, summaryFromRow(summaryRow), nil
}

func (r *ClickHouseRepository) GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]Span, error) {
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT s.span_id, s.parent_span_id, s.trace_id, s.name as operation_name, s.service_name AS service_name, s.kind_string as span_kind,
		       s.timestamp as start_time,
		       s.duration_nano as duration_nano,
		       s.duration_nano / 1000000.0 as duration_ms,
		       s.status_code_string as status, s.status_message,
		       s.http_method, s.http_url, s.response_status_code as http_status_code,
		       s.mat_host_name as host, s.mat_k8s_pod_name as pod, toJSONString(s.attributes) as attributes
		FROM observability.spans s
		WHERE s.team_id = ? AND s.trace_id = ?
		ORDER BY s.timestamp ASC
		LIMIT 5000
	`), uint32(teamID), traceID)
	if err != nil {
		return nil, err
	}
	return spansFromRows(rows), nil
}

func (r *ClickHouseRepository) GetSpanTree(ctx context.Context, teamID int64, spanID string) ([]Span, error) {
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT s.span_id, s.parent_span_id, s.trace_id, s.name as operation_name, s.service_name AS service_name, s.kind_string as span_kind,
		       s.timestamp as start_time,
		       s.duration_nano as duration_nano,
		       s.duration_nano / 1000000.0 as duration_ms,
		       s.status_code_string as status, s.status_message,
		       s.http_method, s.http_url, s.response_status_code as http_status_code,
		       s.mat_host_name as host, s.mat_k8s_pod_name as pod, toJSONString(s.attributes) as attributes
		FROM observability.spans s
		WHERE s.team_id = ?
		  AND s.trace_id = (
		      SELECT trace_id FROM observability.spans WHERE team_id = ? AND span_id = ? LIMIT 1
		  )
		ORDER BY s.timestamp ASC
		LIMIT 5000
	`), uint32(teamID), uint32(teamID), spanID)
	if err != nil {
		return nil, err
	}
	return spansFromRows(rows), nil
}

func (r *ClickHouseRepository) GetServiceDependencies(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceDependency, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s1.service_name AS source,
		       s2.service_name AS target,
		       count()         AS call_count
		FROM observability.spans s1
		JOIN observability.spans s2 ON s1.team_id = s2.team_id AND s1.trace_id = s2.trace_id AND s1.span_id = s2.parent_span_id
			AND s2.ts_bucket_start BETWEEN ? AND ? AND s2.timestamp BETWEEN ? AND ?
		WHERE s1.team_id = ? AND s1.ts_bucket_start BETWEEN ? AND ? AND s1.kind = 3 AND s1.timestamp BETWEEN ? AND ?
		  AND s1.service_name != s2.service_name
		GROUP BY s1.service_name, s2.service_name
		ORDER BY call_count DESC
		LIMIT 100
	`, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
		uint32(teamID), timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	deps := make([]ServiceDependency, 0, len(rows))
	for _, row := range rows {
		deps = append(deps, ServiceDependency{
			Source:    dbutil.StringFromAny(row["source"]),
			Target:    dbutil.StringFromAny(row["target"]),
			CallCount: dbutil.Int64FromAny(row["call_count"]),
		})
	}
	return deps, nil
}

// --- helpers ---

// traceSelectColumns returns the SELECT column list.
// In "all" mode (span-level search), include parent_span_id and span_kind.
func traceSelectColumns(searchMode string) string {
	base := `s.span_id, s.trace_id, s.service_name AS service_name, s.name as operation_name,
		       s.timestamp as start_time, s.duration_nano as duration_nano,
		       s.duration_nano / 1000000.0 as duration_ms,
		       s.status_code_string as status, s.http_method, s.response_status_code as http_status_code,
		       s.status_message`
	if searchMode == "all" {
		base += `, s.parent_span_id, s.kind_string as span_kind`
	}
	return base
}

func traceFromRow(row map[string]any) Trace {
	startTime := dbutil.TimeFromAny(row["start_time"])
	durationNano := dbutil.Int64FromAny(row["duration_nano"])
	return Trace{
		SpanID:         dbutil.StringFromAny(row["span_id"]),
		TraceID:        dbutil.StringFromAny(row["trace_id"]),
		ServiceName:    dbutil.StringFromAny(row["service_name"]),
		OperationName:  dbutil.StringFromAny(row["operation_name"]),
		StartTime:      startTime,
		EndTime:        startTime.Add(time.Duration(durationNano)),
		DurationMs:     dbutil.Float64FromAny(row["duration_ms"]),
		Status:         dbutil.StringFromAny(row["status"]),
		StatusMessage:  dbutil.StringFromAny(row["status_message"]),
		HTTPMethod:     dbutil.StringFromAny(row["http_method"]),
		HTTPStatusCode: int(dbutil.Int64FromAny(row["http_status_code"])),
		ParentSpanID:   dbutil.StringFromAny(row["parent_span_id"]),
		SpanKind:       dbutil.StringFromAny(row["span_kind"]),
	}
}

func summaryFromRow(row map[string]any) TraceSummary {
	return TraceSummary{
		TotalTraces: dbutil.Int64FromAny(row["total_traces"]),
		ErrorTraces: dbutil.Int64FromAny(row["error_traces"]),
		AvgDuration: dbutil.Float64FromAny(row["avg_duration"]),
		P50Duration: dbutil.Float64FromAny(row["p50_duration"]),
		P95Duration: dbutil.Float64FromAny(row["p95_duration"]),
		P99Duration: dbutil.Float64FromAny(row["p99_duration"]),
	}
}

func spansFromRows(rows []map[string]any) []Span {
	spans := make([]Span, 0, len(rows))
	for _, row := range rows {
		startTime := dbutil.TimeFromAny(row["start_time"])
		durationNano := dbutil.Int64FromAny(row["duration_nano"])
		spans = append(spans, Span{
			SpanID:         dbutil.StringFromAny(row["span_id"]),
			ParentSpanID:   dbutil.StringFromAny(row["parent_span_id"]),
			TraceID:        dbutil.StringFromAny(row["trace_id"]),
			OperationName:  dbutil.StringFromAny(row["operation_name"]),
			ServiceName:    dbutil.StringFromAny(row["service_name"]),
			SpanKind:       dbutil.StringFromAny(row["span_kind"]),
			StartTime:      startTime,
			EndTime:        startTime.Add(time.Duration(durationNano)),
			DurationMs:     dbutil.Float64FromAny(row["duration_ms"]),
			Status:         dbutil.StringFromAny(row["status"]),
			StatusMessage:  dbutil.StringFromAny(row["status_message"]),
			HTTPMethod:     dbutil.StringFromAny(row["http_method"]),
			HTTPURL:        dbutil.StringFromAny(row["http_url"]),
			HTTPStatusCode: int(dbutil.Int64FromAny(row["http_status_code"])),
			Host:           dbutil.StringFromAny(row["host"]),
			Pod:            dbutil.StringFromAny(row["pod"]),
			Attributes:     dbutil.StringFromAny(row["attributes"]),
		})
	}
	return spans
}
