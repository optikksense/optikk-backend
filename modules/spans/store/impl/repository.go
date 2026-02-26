package impl

import (
	"context"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/modules/spans/model"
)

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) buildTraceQueryArgs(f model.TraceFilters) (string, []any) {
	queryFrag := ` WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{f.TeamUUID, dbutil.SqlTime(f.StartMs), dbutil.SqlTime(f.EndMs)}

	if len(f.Services) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Services)
		queryFrag += ` AND service_name IN ` + in
		args = append(args, vals...)
	}
	if f.Status != "" {
		queryFrag += ` AND status = ?`
		args = append(args, f.Status)
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
	if f.HTTPStatus != "" {
		queryFrag += ` AND http_status_code = ?`
		args = append(args, dbutil.MustAtoi64(f.HTTPStatus, 0))
	}
	return queryFrag, args
}

func (r *ClickHouseRepository) GetTraces(ctx context.Context, f model.TraceFilters, limit, offset int) ([]model.Trace, int64, model.TraceSummary, error) {
	queryFrag, args := r.buildTraceQueryArgs(f)

	query := `
		SELECT trace_id, service_name, operation_name, start_time, end_time, duration_ms,
		       status, http_method, http_status_code
		FROM spans` + queryFrag + ` ORDER BY start_time DESC LIMIT ? OFFSET ?`
	traceArgs := append(args, limit, offset)

	rows, err := dbutil.QueryMaps(r.db, query, traceArgs...)
	if err != nil {
		return nil, 0, model.TraceSummary{}, err
	}

	traces := make([]model.Trace, 0, len(rows))
	for _, row := range rows {
		traces = append(traces, model.Trace{
			TraceID:        dbutil.StringFromAny(row["trace_id"]),
			ServiceName:    dbutil.StringFromAny(row["service_name"]),
			OperationName:  dbutil.StringFromAny(row["operation_name"]),
			StartTime:      row["start_time"].(time.Time),
			EndTime:        row["end_time"].(time.Time),
			DurationMs:     dbutil.Float64FromAny(row["duration_ms"]),
			Status:         dbutil.StringFromAny(row["status"]),
			HTTPMethod:     dbutil.StringFromAny(row["http_method"]),
			HTTPStatusCode: int(dbutil.Int64FromAny(row["http_status_code"])),
		})
	}

	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM spans`+queryFrag, args...)

	summaryRow, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as total_traces,
		       sum(if(status = 'ERROR', 1, 0)) as error_traces,
		       AVG(duration_ms) as avg_duration,
		       quantile(0.5)(duration_ms) as p50_duration,
		       quantile(0.95)(duration_ms) as p95_duration,
		       quantile(0.99)(duration_ms) as p99_duration
		FROM spans`+queryFrag, args...)
	if err != nil {
		return traces, total, model.TraceSummary{}, err
	}

	summary := model.TraceSummary{
		TotalTraces: dbutil.Int64FromAny(summaryRow["total_traces"]),
		ErrorTraces: dbutil.Int64FromAny(summaryRow["error_traces"]),
		AvgDuration: dbutil.Float64FromAny(summaryRow["avg_duration"]),
		P50Duration: dbutil.Float64FromAny(summaryRow["p50_duration"]),
		P95Duration: dbutil.Float64FromAny(summaryRow["p95_duration"]),
		P99Duration: dbutil.Float64FromAny(summaryRow["p99_duration"]),
	}

	return traces, total, summary, nil
}

func (r *ClickHouseRepository) GetTraceSpans(ctx context.Context, teamUUID, traceID string) ([]model.Span, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT span_id, parent_span_id, trace_id, operation_name, service_name, span_kind,
		       start_time, end_time, duration_ms, status, status_message,
		       http_method, http_url, http_status_code, host, pod, attributes
		FROM spans
		WHERE team_id = ? AND trace_id = ?
		ORDER BY start_time ASC
	`, teamUUID, traceID)
	if err != nil {
		return nil, err
	}

	spans := make([]model.Span, 0, len(rows))
	for _, row := range rows {
		spans = append(spans, model.Span{
			SpanID:         dbutil.StringFromAny(row["span_id"]),
			ParentSpanID:   dbutil.StringFromAny(row["parent_span_id"]),
			TraceID:        dbutil.StringFromAny(row["trace_id"]),
			OperationName:  dbutil.StringFromAny(row["operation_name"]),
			ServiceName:    dbutil.StringFromAny(row["service_name"]),
			SpanKind:       dbutil.StringFromAny(row["span_kind"]),
			StartTime:      row["start_time"].(time.Time),
			EndTime:        row["end_time"].(time.Time),
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

func (r *ClickHouseRepository) GetServiceDependencies(ctx context.Context, teamUUID string, startMs, endMs int64) ([]model.ServiceDependency, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT p.service_name as source,
		       c.service_name as target,
		       COUNT(*) as call_count
		FROM spans c
		JOIN spans p ON c.parent_span_id = p.span_id AND c.trace_id = p.trace_id AND c.team_id = p.team_id
		WHERE c.team_id = ?
		  AND c.start_time BETWEEN ? AND ?
		  AND p.service_name != c.service_name
		GROUP BY p.service_name, c.service_name
		ORDER BY call_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	deps := make([]model.ServiceDependency, 0, len(rows))
	for _, row := range rows {
		deps = append(deps, model.ServiceDependency{
			Source:    dbutil.StringFromAny(row["source"]),
			Target:    dbutil.StringFromAny(row["target"]),
			CallCount: dbutil.Int64FromAny(row["call_count"]),
		})
	}
	return deps, nil
}

func (r *ClickHouseRepository) GetErrorGroups(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]model.ErrorGroup, error) {
	query := `
		SELECT service_name, operation_name, status_message, http_status_code,
		       COUNT(*) as error_count,
		       MAX(start_time) as last_occurrence,
		       MIN(start_time) as first_occurrence,
		       (groupArray(trace_id) as trace_ids)[1] as sample_trace_id
		FROM spans
		WHERE team_id = ? AND status = 'ERROR' AND start_time BETWEEN ? AND ?`
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

	groups := make([]model.ErrorGroup, 0, len(rows))
	for _, row := range rows {
		groups = append(groups, model.ErrorGroup{
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			OperationName:   dbutil.StringFromAny(row["operation_name"]),
			StatusMessage:   dbutil.StringFromAny(row["status_message"]),
			HTTPStatusCode:  int(dbutil.Int64FromAny(row["http_status_code"])),
			ErrorCount:      dbutil.Int64FromAny(row["error_count"]),
			LastOccurrence:  row["last_occurrence"].(time.Time),
			FirstOccurrence: row["first_occurrence"].(time.Time),
			SampleTraceID:   dbutil.StringFromAny(row["sample_trace_id"]),
		})
	}
	return groups, nil
}

func (r *ClickHouseRepository) GetErrorTimeSeries(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string) ([]model.ErrorTimeSeries, error) {
	query := `
		SELECT service_name,
		       toStartOfMinute(start_time) as timestamp,
		       COUNT(*) as total_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       if(COUNT(*) > 0, sum(if(status='ERROR', 1, 0))*100.0/COUNT(*), 0) as error_rate
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, toStartOfMinute(start_time)
	           ORDER BY timestamp ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]model.ErrorTimeSeries, 0, len(rows))
	for _, row := range rows {
		points = append(points, model.ErrorTimeSeries{
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			Timestamp:   row["timestamp"].(time.Time),
			TotalCount:  dbutil.Int64FromAny(row["total_count"]),
			ErrorCount:  dbutil.Int64FromAny(row["error_count"]),
			ErrorRate:   dbutil.Float64FromAny(row["error_rate"]),
		})
	}
	return points, nil
}

func (r *ClickHouseRepository) GetLatencyHistogram(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName, operationName string) ([]model.LatencyHistogramBucket, error) {
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

	buckets := make([]model.LatencyHistogramBucket, 0, len(rows))
	for _, row := range rows {
		min := dbutil.Int64FromAny(row["bucket_min"])
		buckets = append(buckets, model.LatencyHistogramBucket{
			BucketLabel: dbutil.StringFromAny(row["bucket_label"]),
			BucketMin:   min,
			BucketMax:   min + 1, // Simple max placeholder for now
			SpanCount:   dbutil.Int64FromAny(row["span_count"]),
		})
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetLatencyHeatmap(ctx context.Context, teamUUID string, startMs, endMs int64, serviceName string) ([]model.LatencyHeatmapPoint, error) {
	query := `
		SELECT toStartOfMinute(start_time) as time_bucket,
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
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY toStartOfMinute(start_time), latency_bucket
	           ORDER BY time_bucket ASC, latency_bucket ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]model.LatencyHeatmapPoint, 0, len(rows))
	for _, row := range rows {
		points = append(points, model.LatencyHeatmapPoint{
			TimeBucket:    row["time_bucket"].(time.Time),
			LatencyBucket: dbutil.StringFromAny(row["latency_bucket"]),
			SpanCount:     dbutil.Int64FromAny(row["span_count"]),
		})
	}
	return points, nil
}
