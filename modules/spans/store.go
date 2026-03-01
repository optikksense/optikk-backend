package traces

import (
	"context"
	"fmt"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// ClickHouseRepository is the data access layer for traces.
type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// timeBucketExpr returns a ClickHouse expression grouping the `minute` column by
// adaptive granularity. The spans_service_1m view stores 1-minute rows, so coarser
// groupings are a free second-level aggregation over already-aggregated data.
func timeBucketExpr(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return "minute"
	case hours <= 24:
		return "toStartOfFiveMinutes(minute)"
	case hours <= 168:
		return "toStartOfHour(minute)"
	default:
		return "toStartOfDay(minute)"
	}
}

// rawTimeBucketExpr returns a ClickHouse expression grouping a raw timestamp column.
func rawTimeBucketExpr(startMs, endMs int64, column string) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return fmt.Sprintf("toStartOfMinute(%s)", column)
	case hours <= 24:
		return fmt.Sprintf("toStartOfFiveMinutes(%s)", column)
	case hours <= 168:
		return fmt.Sprintf("toStartOfHour(%s)", column)
	default:
		return fmt.Sprintf("toStartOfDay(%s)", column)
	}
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

	queryFrag := ` WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{f.TeamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}

	if len(f.Services) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Services)
		queryFrag += ` AND service_name IN ` + in
		args = append(args, vals...)
	}
	if f.Status != "" {
		if f.Status == "ERROR" {
			queryFrag += ` AND (status = 'ERROR' OR http_status_code >= 400)`
		} else {
			queryFrag += ` AND status = ?`
			args = append(args, f.Status)
		}
	}
	if f.MinDuration != "" {
		queryFrag += ` AND duration_ms >= ?`
		args = append(args, dbutil.MustAtoi64(f.MinDuration, 0))
	}
	if f.MaxDuration != "" {
		queryFrag += ` AND duration_ms <= ?`
		args = append(args, dbutil.MustAtoi64(f.MaxDuration, 0))
	}
	if f.TraceID != "" {
		queryFrag += ` AND trace_id = ?`
		args = append(args, f.TraceID)
	}
	if f.Operation != "" {
		queryFrag += ` AND operation_name LIKE ?`
		args = append(args, "%"+f.Operation+"%")
	}
	if f.HTTPMethod != "" {
		queryFrag += ` AND upper(http_method) = upper(?)`
		args = append(args, f.HTTPMethod)
	}
	if f.HTTPStatus != "" {
		queryFrag += ` AND http_status_code = ?`
		args = append(args, dbutil.MustAtoi64(f.HTTPStatus, 0))
	}
	return queryFrag, args
}

func (r *ClickHouseRepository) GetTraces(ctx context.Context, f TraceFilters, limit, offset int) ([]Trace, int64, TraceSummary, error) {
	queryFrag, args := r.buildTraceQueryArgs(f)

	query := `
		SELECT span_id, trace_id, service_name, operation_name, start_time, end_time, duration_ms,
		       status, http_method, http_status_code
		FROM spans` + queryFrag + ` ORDER BY start_time DESC LIMIT ? OFFSET ?`
	traceArgs := append(args, limit, offset)

	rows, err := dbutil.QueryMaps(r.db, query, traceArgs...)
	if err != nil {
		return nil, 0, TraceSummary{}, err
	}

	traces := make([]Trace, 0, len(rows))
	for _, row := range rows {
		traces = append(traces, Trace{
			SpanID:         dbutil.StringFromAny(row["span_id"]),
			TraceID:        dbutil.StringFromAny(row["trace_id"]),
			ServiceName:    dbutil.StringFromAny(row["service_name"]),
			OperationName:  dbutil.StringFromAny(row["operation_name"]),
			StartTime:      dbutil.TimeFromAny(row["start_time"]),
			EndTime:        dbutil.TimeFromAny(row["end_time"]),
			DurationMs:     dbutil.Float64FromAny(row["duration_ms"]),
			Status:         dbutil.StringFromAny(row["status"]),
			HTTPMethod:     dbutil.StringFromAny(row["http_method"]),
			HTTPStatusCode: int(dbutil.Int64FromAny(row["http_status_code"])),
		})
	}

	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM spans`+queryFrag, args...)

	summaryRow, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as total_traces,
		       sum(if(status = 'ERROR' OR http_status_code >= 400, 1, 0)) as error_traces,
		       AVG(duration_ms) as avg_duration,
		       quantile(0.5)(duration_ms) as p50_duration,
		       quantile(0.95)(duration_ms) as p95_duration,
		       quantile(0.99)(duration_ms) as p99_duration
		FROM spans`+queryFrag, args...)
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

func (r *ClickHouseRepository) GetTraceSpans(ctx context.Context, teamUUID, traceID string) ([]Span, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT span_id, parent_span_id, trace_id, operation_name, service_name, span_kind,
		       start_time, end_time, duration_ms, status, status_message,
		       http_method, http_url, http_status_code, host, pod, attributes
		FROM spans
		WHERE team_id = ? AND trace_id = ?
		ORDER BY start_time ASC
		LIMIT 10000
	`, teamUUID, traceID)
	if err != nil {
		return nil, err
	}

	spans := make([]Span, 0, len(rows))
	for _, row := range rows {
		spans = append(spans, Span{
			SpanID:         dbutil.StringFromAny(row["span_id"]),
			ParentSpanID:   dbutil.StringFromAny(row["parent_span_id"]),
			TraceID:        dbutil.StringFromAny(row["trace_id"]),
			OperationName:  dbutil.StringFromAny(row["operation_name"]),
			ServiceName:    dbutil.StringFromAny(row["service_name"]),
			SpanKind:       dbutil.StringFromAny(row["span_kind"]),
			StartTime:      dbutil.TimeFromAny(row["start_time"]),
			EndTime:        dbutil.TimeFromAny(row["end_time"]),
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
// belonging to that trace in a single query, ordered by start_time ascending.
func (r *ClickHouseRepository) GetSpanTree(ctx context.Context, teamUUID, spanID string) ([]Span, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT span_id, parent_span_id, trace_id, operation_name, service_name, span_kind,
		       start_time, end_time, duration_ms, status, status_message,
		       http_method, http_url, http_status_code, host, pod, attributes
		FROM spans
		WHERE team_id = ?
		  AND trace_id = (
		      SELECT trace_id FROM spans WHERE team_id = ? AND span_id = ? LIMIT 1
		  )
		ORDER BY start_time ASC
		LIMIT 10000
	`, teamUUID, teamUUID, spanID)
	if err != nil {
		return nil, err
	}

	spans := make([]Span, 0, len(rows))
	for _, row := range rows {
		spans = append(spans, Span{
			SpanID:         dbutil.StringFromAny(row["span_id"]),
			ParentSpanID:   dbutil.StringFromAny(row["parent_span_id"]),
			TraceID:        dbutil.StringFromAny(row["trace_id"]),
			OperationName:  dbutil.StringFromAny(row["operation_name"]),
			ServiceName:    dbutil.StringFromAny(row["service_name"]),
			SpanKind:       dbutil.StringFromAny(row["span_kind"]),
			StartTime:      dbutil.TimeFromAny(row["start_time"]),
			EndTime:        dbutil.TimeFromAny(row["end_time"]),
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

// GetServiceDependencies reads from spans_edges_1m (pre-aggregated, no JOIN).
func (r *ClickHouseRepository) GetServiceDependencies(ctx context.Context, teamUUID string, startMs, endMs int64) ([]ServiceDependency, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT parent_service_name AS source,
		       service_name        AS target,
		       countMerge(call_count) AS call_count
		FROM observability.spans_edges_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
		GROUP BY parent_service_name, service_name
		ORDER BY call_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetErrorGroups(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	query := `
		SELECT service_name, operation_name, status_message, http_status_code,
		       COUNT(*) as error_count,
		       MAX(start_time) as last_occurrence,
		       MIN(start_time) as first_occurrence,
		       (groupArray(trace_id) as trace_ids)[1] as sample_trace_id
		FROM spans
		WHERE team_id = ? AND (status = 'ERROR' OR http_status_code >= 400) AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, operation_name, status_message, http_status_code
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

// GetErrorTimeSeries reads from spans_service_1m with adaptive time bucketing.
func (r *ClickHouseRepository) GetErrorTimeSeries(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string) ([]ErrorTimeSeries, error) {
	bucket := timeBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name,
		       timestamp,
		       total_count,
		       error_count,
		       if(total_count > 0, error_count*100.0/total_count, 0) AS error_rate
		FROM (
			SELECT service_name,
			       %s AS timestamp,
			       countMerge(request_count) AS total_count,
			       countIfMerge(error_count) AS error_count
			FROM observability.spans_service_1m
			WHERE team_id = ? AND minute BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY service_name, %s
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

func (r *ClickHouseRepository) GetLatencyHistogram(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName, operationName string) ([]LatencyHistogramBucket, error) {
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
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	if operationName != "" {
		query += ` AND operation_name = ?`
		args = append(args, operationName)
	}
	query += ` GROUP BY bucket_label, bucket_min ORDER BY bucket_min ASC`

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

func (r *ClickHouseRepository) GetLatencyHeatmap(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string) ([]LatencyHeatmapPoint, error) {
	bucket := rawTimeBucketExpr(startMs, endMs, "start_time")
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
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY %s, latency_bucket
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
