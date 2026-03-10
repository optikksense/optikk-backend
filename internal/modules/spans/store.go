package traces

import (
	"context"
	"fmt"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// ClickHouseRepository is the data access layer for traces.
type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// rawTimeBucketExpr returns a ClickHouse expression grouping a raw timestamp column.
// Delegates to the shared timebucket package.
func rawTimeBucketExpr(startMs, endMs int64, column string) string {
	return timebucket.ExprForColumn(startMs, endMs, column)
}

func (r *ClickHouseRepository) buildTraceQueryArgs(f TraceFilters) (string, []any) {
	const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000 // 30 days max window
	startMs := f.StartMs
	endMs := f.EndMs
	if endMs <= 0 {
		endMs = time.Now().UnixMilli()
	}
	// Prevent unbounded or massively expensive full-table scans
	if startMs <= 0 || (endMs-startMs) > maxTimeRangeMs {
		startMs = endMs - maxTimeRangeMs
	}

	queryFrag := ` WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.parent_span_id = '' AND s.timestamp BETWEEN ? AND ?`
	args := []any{uint32(f.TeamID), uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}

	if len(f.Services) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Services)
		queryFrag += ` AND s.service_name IN ` + in
		args = append(args, vals...)
	}
	if f.Status != "" {
		if f.Status == "ERROR" {
			queryFrag += ` AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)`
		} else {
			queryFrag += ` AND s.status_code_string = ?`
			args = append(args, f.Status)
		}
	}
	if f.MinDuration != "" {
		queryFrag += ` AND s.duration_nano >= ?`
		args = append(args, dbutil.MustAtoi64(f.MinDuration, 0)*1000000) // Convert ms to ns
	}
	if f.MaxDuration != "" {
		queryFrag += ` AND s.duration_nano <= ?`
		args = append(args, dbutil.MustAtoi64(f.MaxDuration, 0)*1000000) // Convert ms to ns
	}
	if f.TraceID != "" {
		queryFrag += ` AND s.trace_id = ?`
		args = append(args, f.TraceID)
	}
	if f.Operation != "" {
		queryFrag += ` AND s.name LIKE ?`
		args = append(args, "%"+f.Operation+"%")
	}
	if f.HTTPMethod != "" {
		queryFrag += ` AND upper(s.http_method) = upper(?)`
		args = append(args, f.HTTPMethod)
	}
	if f.HTTPStatus != "" {
		queryFrag += ` AND s.response_status_code = ?`
		args = append(args, f.HTTPStatus)
	}
	for _, af := range f.AttributeFilters {
		switch af.Op {
		case "neq":
			queryFrag += ` AND s.attributes_string[?] != ?`
			args = append(args, af.Key, af.Value)
		case "contains":
			queryFrag += ` AND positionCaseInsensitive(s.attributes_string[?], ?) > 0`
			args = append(args, af.Key, af.Value)
		case "regex":
			queryFrag += ` AND match(s.attributes_string[?], ?)`
			args = append(args, af.Key, af.Value)
		default: // "eq"
			queryFrag += ` AND s.attributes_string[?] = ?`
			args = append(args, af.Key, af.Value)
		}
	}
	return queryFrag, args
}

// GetTracesKeyset returns a page of traces using keyset (cursor) pagination instead of OFFSET.
// cursor is the (timestamp, spanId) of the last row seen; empty cursor returns the first page.
func (r *ClickHouseRepository) GetTracesKeyset(ctx context.Context, f TraceFilters, limit int, cursor TraceCursor) ([]Trace, TraceSummary, bool, error) {
	queryFrag, args := r.buildTraceQueryArgs(f)

	// Apply cursor condition for stable keyset pagination
	if !cursor.Timestamp.IsZero() && cursor.SpanID != "" {
		queryFrag += ` AND (s.timestamp < ? OR (s.timestamp = ? AND s.span_id < ?))`
		args = append(args, cursor.Timestamp, cursor.Timestamp, cursor.SpanID)
	}

	// Fetch limit+1 to determine hasMore
	query := `
		SELECT s.span_id, s.trace_id, s.service_name AS service_name, s.name as operation_name,
		       s.timestamp as start_time, s.duration_nano as duration_nano,
		       s.duration_nano / 1000000.0 as duration_ms,
		       s.status_code_string as status, s.http_method, s.response_status_code as http_status_code,
		       s.status_message
		FROM observability.spans s` + queryFrag + ` ORDER BY s.timestamp DESC, s.span_id DESC LIMIT ?`
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
		startTime := dbutil.TimeFromAny(row["start_time"])
		durationNano := dbutil.Int64FromAny(row["duration_nano"])
		traces = append(traces, Trace{
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
		})
	}

	// Summary stats use the same filters but without the cursor condition.
	baseArgs := r.buildTraceQueryArgsArgs(f)
	summaryRow, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as total_traces,
		       sum(if(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400, 1, 0)) as error_traces,
		       AVG(s.duration_nano / 1000000.0) as avg_duration,
		       quantile(0.5)(s.duration_nano / 1000000.0) as p50_duration,
		       quantile(0.95)(s.duration_nano / 1000000.0) as p95_duration,
		       quantile(0.99)(s.duration_nano / 1000000.0) as p99_duration
		FROM observability.spans s`+r.buildTraceQueryFrag(f), baseArgs...)
	if err != nil {
		return traces, TraceSummary{}, hasMore, nil
	}

	summary := TraceSummary{
		TotalTraces: dbutil.Int64FromAny(summaryRow["total_traces"]),
		ErrorTraces: dbutil.Int64FromAny(summaryRow["error_traces"]),
		AvgDuration: dbutil.Float64FromAny(summaryRow["avg_duration"]),
		P50Duration: dbutil.Float64FromAny(summaryRow["p50_duration"]),
		P95Duration: dbutil.Float64FromAny(summaryRow["p95_duration"]),
		P99Duration: dbutil.Float64FromAny(summaryRow["p99_duration"]),
	}
	return traces, summary, hasMore, nil
}

// buildTraceQueryFrag and buildTraceQueryArgsArgs are helpers to avoid re-running
// the cursor-augmented WHERE for the summary query.
func (r *ClickHouseRepository) buildTraceQueryFrag(f TraceFilters) string {
	frag, _ := r.buildTraceQueryArgs(f)
	return frag
}
func (r *ClickHouseRepository) buildTraceQueryArgsArgs(f TraceFilters) []any {
	_, args := r.buildTraceQueryArgs(f)
	return args
}

// GetOperationAggregation returns per-operation RED metrics for the given filters.
// This is the "aggregated traces" view used by the operation-level table on the frontend.
func (r *ClickHouseRepository) GetOperationAggregation(ctx context.Context, f TraceFilters, limit int) ([]TraceOperationRow, error) {
	queryFrag, args := r.buildTraceQueryArgs(f)

	query := fmt.Sprintf(`
		SELECT s.service_name,
		       s.name AS operation_name,
		       count() AS span_count,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) AS error_count,
		       if(count() > 0,
		          countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count(),
		          0) AS error_rate,
		       quantile(0.5)(s.duration_nano / 1000000.0) AS p50_ms,
		       quantile(0.95)(s.duration_nano / 1000000.0) AS p95_ms,
		       quantile(0.99)(s.duration_nano / 1000000.0) AS p99_ms,
		       avg(s.duration_nano / 1000000.0) AS avg_ms
		FROM observability.spans s%s
		GROUP BY s.service_name, s.name
		ORDER BY span_count DESC
		LIMIT ?`, queryFrag)

	rows, err := dbutil.QueryMaps(r.db, query, append(args, limit)...)
	if err != nil {
		return nil, err
	}

	result := make([]TraceOperationRow, 0, len(rows))
	for _, row := range rows {
		result = append(result, TraceOperationRow{
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			SpanCount:     dbutil.Int64FromAny(row["span_count"]),
			ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
			ErrorRate:     dbutil.Float64FromAny(row["error_rate"]),
			P50Ms:         dbutil.Float64FromAny(row["p50_ms"]),
			P95Ms:         dbutil.Float64FromAny(row["p95_ms"]),
			P99Ms:         dbutil.Float64FromAny(row["p99_ms"]),
			AvgMs:         dbutil.Float64FromAny(row["avg_ms"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetTraces(ctx context.Context, f TraceFilters, limit, offset int) ([]Trace, int64, TraceSummary, error) {
	queryFrag, args := r.buildTraceQueryArgs(f)

	query := `
		SELECT s.span_id, s.trace_id, s.service_name AS service_name, s.name as operation_name, s.timestamp as start_time,
		       s.duration_nano as duration_nano,
		       s.duration_nano / 1000000.0 as duration_ms,
		       s.status_code_string as status, s.http_method, s.response_status_code as http_status_code
		FROM observability.spans s` + queryFrag + ` ORDER BY s.timestamp DESC LIMIT ? OFFSET ?`
	traceArgs := append(args, limit, offset)

	rows, err := dbutil.QueryMaps(r.db, query, traceArgs...)
	if err != nil {
		return nil, 0, TraceSummary{}, err
	}

	traces := make([]Trace, 0, len(rows))
	for _, row := range rows {
		startTime := dbutil.TimeFromAny(row["start_time"])
		durationNano := dbutil.Int64FromAny(row["duration_nano"])
		traces = append(traces, Trace{
			SpanID:         dbutil.StringFromAny(row["span_id"]),
			TraceID:        dbutil.StringFromAny(row["trace_id"]),
			ServiceName:    dbutil.StringFromAny(row["service_name"]),
			OperationName:  dbutil.StringFromAny(row["operation_name"]),
			StartTime:      startTime,
			EndTime:        startTime.Add(time.Duration(durationNano)),
			DurationMs:     dbutil.Float64FromAny(row["duration_ms"]),
			Status:         dbutil.StringFromAny(row["status"]),
			HTTPMethod:     dbutil.StringFromAny(row["http_method"]),
			HTTPStatusCode: int(dbutil.Int64FromAny(row["http_status_code"])),
		})
	}

	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM observability.spans s`+queryFrag, args...)

	summaryRow, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as total_traces,
		       sum(if(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400, 1, 0)) as error_traces,
		       AVG(s.duration_nano / 1000000.0) as avg_duration,
		       quantile(0.5)(s.duration_nano / 1000000.0) as p50_duration,
		       quantile(0.95)(s.duration_nano / 1000000.0) as p95_duration,
		       quantile(0.99)(s.duration_nano / 1000000.0) as p99_duration
		FROM observability.spans s`+queryFrag, args...)
	if err != nil {
		return traces, total, TraceSummary{}, err
	}

	summary := TraceSummary{
		TotalTraces: dbutil.Int64FromAny(summaryRow["total_traces"]),
		ErrorTraces: dbutil.Int64FromAny(summaryRow["error_traces"]),
		AvgDuration: dbutil.Float64FromAny(summaryRow["avg_duration"]),
		P50Duration: dbutil.Float64FromAny(summaryRow["p50_duration"]),
		P95Duration: dbutil.Float64FromAny(summaryRow["p95_duration"]),
		P99Duration: dbutil.Float64FromAny(summaryRow["p99_duration"]),
	}

	return traces, total, summary, nil
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
	return spans, nil
}

// GetSpanTree resolves the trace_id for the given root spanID and returns all spans
// belonging to that trace in a single query, ordered by timestamp ascending.
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
	return spans, nil
}

// GetServiceDependencies reads from raw spans table.
// Note: parent_service_name is no longer stored, so this query needs to be reworked
// to infer dependencies from CLIENT/SERVER span pairs or use a different approach.
func (r *ClickHouseRepository) GetServiceDependencies(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceDependency, error) {
	// Simplified approach: use CLIENT spans to infer dependencies
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
	`, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
		uint32(teamID), uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	query := `
		SELECT s.service_name AS service_name, s.name as operation_name, s.status_message, s.response_status_code as http_status_code,
		       COUNT(*) as error_count,
		       MAX(s.timestamp) as last_occurrence,
		       MIN(s.timestamp) as first_occurrence,
		       (groupArray(s.trace_id) as trace_ids)[1] as sample_trace_id
		FROM observability.spans s
		WHERE s.team_id = ? AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`
	args := []any{teamID, uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY s.service_name, s.name, s.status_message, s.response_status_code
	           ORDER BY error_count DESC LIMIT ?`
	args = append(args, limit)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	groups := make([]ErrorGroup, 0, len(rows))
	for _, row := range rows {
		groups = append(groups, ErrorGroup{
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			OperationName:   dbutil.StringFromAny(row["operation_name"]),
			StatusMessage:   dbutil.StringFromAny(row["status_message"]),
			HTTPStatusCode:  int(dbutil.Int64FromAny(row["http_status_code"])),
			ErrorCount:      dbutil.Int64FromAny(row["error_count"]),
			LastOccurrence:  dbutil.TimeFromAny(row["last_occurrence"]),
			FirstOccurrence: dbutil.TimeFromAny(row["first_occurrence"]),
			SampleTraceID:   dbutil.StringFromAny(row["sample_trace_id"]),
		})
	}
	return groups, nil
}

// GetErrorTimeSeries reads from raw spans with adaptive time bucketing.
func (r *ClickHouseRepository) GetErrorTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]ErrorTimeSeries, error) {
	bucket := rawTimeBucketExpr(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT service_name,
		       timestamp,
		       total_count,
		       error_count,
		       if(total_count > 0, error_count*100.0/total_count, 0) AS error_rate
		FROM (
			SELECT s.service_name AS service_name,
			       %s AS timestamp,
			       count()                   AS total_count,
			       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) AS error_count
			FROM observability.spans s
			WHERE s.team_id = ? AND s.parent_span_id = '' AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamID, uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY s.service_name, %s
		)
		ORDER BY timestamp ASC`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]ErrorTimeSeries, 0, len(rows))
	for _, row := range rows {
		points = append(points, ErrorTimeSeries{
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			Timestamp:   dbutil.TimeFromAny(row["timestamp"]),
			TotalCount:  dbutil.Int64FromAny(row["total_count"]),
			ErrorCount:  dbutil.Int64FromAny(row["error_count"]),
			ErrorRate:   dbutil.Float64FromAny(row["error_rate"]),
		})
	}
	return points, nil
}

func (r *ClickHouseRepository) GetLatencyHistogram(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) ([]LatencyHistogramBucket, error) {
	query := `
		SELECT
			CASE
				WHEN duration_ms < 10 THEN '0-10ms'
				WHEN duration_ms < 25 THEN '10-25ms'
				WHEN duration_ms < 50 THEN '25-50ms'
				WHEN duration_ms < 100 THEN '50-100ms'
				WHEN duration_ms < 250 THEN '100-250ms'
				WHEN duration_ms < 500 THEN '250-500ms'
				WHEN duration_ms < 1000 THEN '500ms-1s'
				WHEN duration_ms < 2500 THEN '1s-2.5s'
				WHEN duration_ms < 5000 THEN '2.5s-5s'
				ELSE '>5s'
			END as bucket_label,
			CASE
				WHEN duration_ms < 10 THEN 0
				WHEN duration_ms < 25 THEN 10
				WHEN duration_ms < 50 THEN 25
				WHEN duration_ms < 100 THEN 50
				WHEN duration_ms < 250 THEN 100
				WHEN duration_ms < 500 THEN 250
				WHEN duration_ms < 1000 THEN 500
				WHEN duration_ms < 2500 THEN 1000
				WHEN duration_ms < 5000 THEN 2500
				ELSE 5000
			END as bucket_min,
			COUNT(*) as span_count
		FROM (
			SELECT s.duration_nano / 1000000.0 as duration_ms
			FROM observability.spans s
			WHERE s.team_id = ? AND s.parent_span_id = '' AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`
	args := []any{teamID, uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	if operationName != "" {
		query += ` AND s.name = ?`
		args = append(args, operationName)
	}
	query += `) GROUP BY bucket_label, bucket_min ORDER BY bucket_min ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	buckets := make([]LatencyHistogramBucket, 0, len(rows))
	for _, row := range rows {
		min := dbutil.Int64FromAny(row["bucket_min"])
		buckets = append(buckets, LatencyHistogramBucket{
			BucketLabel: dbutil.StringFromAny(row["bucket_label"]),
			BucketMin:   min,
			BucketMax:   min + 1,
			SpanCount:   dbutil.Int64FromAny(row["span_count"]),
		})
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]LatencyHeatmapPoint, error) {
	// The outer SELECT groups rows from a subquery that exposes `timestamp`
	// (without the `s.` alias), so use that column name for bucket expressions.
	bucket := rawTimeBucketExpr(startMs, endMs, "timestamp")
	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       CASE
				WHEN duration_ms < 50 THEN '0-50ms'
				WHEN duration_ms < 100 THEN '50-100ms'
				WHEN duration_ms < 250 THEN '100-250ms'
				WHEN duration_ms < 500 THEN '250-500ms'
				WHEN duration_ms < 1000 THEN '500ms-1s'
				ELSE '>1s'
			END as latency_bucket,
			COUNT(*) as span_count
		FROM (
			SELECT s.timestamp, s.duration_nano / 1000000.0 as duration_ms
			FROM observability.spans s
			WHERE s.team_id = ? AND s.parent_span_id = '' AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamID, uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(`) GROUP BY %s, latency_bucket
	           ORDER BY time_bucket ASC, latency_bucket ASC`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]LatencyHeatmapPoint, 0, len(rows))
	for _, row := range rows {
		points = append(points, LatencyHeatmapPoint{
			TimeBucket:    dbutil.TimeFromAny(row["time_bucket"]),
			LatencyBucket: dbutil.StringFromAny(row["latency_bucket"]),
			SpanCount:     dbutil.Int64FromAny(row["span_count"]),
		})
	}
	return points, nil
}
