package traces

import (
	"context"
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

func (r *ClickHouseRepository) GetOperationAggregation(ctx context.Context, f TraceFilters, limit int) ([]TraceOperationRow, error) {
	queryFrag, args := buildWhereClause(f)

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

func (r *ClickHouseRepository) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	query := `
		SELECT s.service_name AS service_name, s.name as operation_name, s.status_message, s.response_status_code as http_status_code,
		       COUNT(*) as error_count,
		       MAX(s.timestamp) as last_occurrence,
		       MIN(s.timestamp) as first_occurrence,
		       (groupArray(s.trace_id) as trace_ids)[1] as sample_trace_id
		FROM observability.spans s
		WHERE s.team_id = ? AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
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
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
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
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
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
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
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
