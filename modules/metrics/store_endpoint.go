package metrics

import (
	"context"
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// GetEndpointMetrics returns aggregate metrics per endpoint.
func (r *ClickHouseRepository) GetEndpointMetrics(ctx context.Context, f MetricFilters) ([]EndpointMetric, error) {
	view := selectEndpointView(f.Start, f.End)
	query := fmt.Sprintf(`
		SELECT service_name, operation_name, http_method,
		       countMerge(request_count)       AS request_count,
		       countMerge(error_count)         AS error_count,
		       avgMerge(avg_state)             AS avg_latency,
		       quantileMerge(0.5)(p50_state)   AS p50_latency,
		       quantileMerge(0.95)(p95_state)  AS p95_latency,
		       quantileMerge(0.99)(p99_state)  AS p99_latency
		FROM %s
		WHERE team_id = ? AND minute BETWEEN ? AND ?`, view)
	args := []any{f.TeamUUID, f.Start, f.End}
	if f.ServiceName != "" {
		query += ` AND service_name = ?`
		args = append(args, f.ServiceName)
	}
	query += ` GROUP BY service_name, operation_name, http_method ORDER BY countMerge(request_count) DESC LIMIT 100`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	metrics := make([]EndpointMetric, 0, len(rows))
	for _, row := range rows {
		metrics = append(metrics, EndpointMetric{
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			HTTPMethod:    dbutil.StringFromAny(row["http_method"]),
			RequestCount:  dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:    dbutil.Float64FromAny(row["avg_latency"]),
			P50Latency:    dbutil.Float64FromAny(row["p50_latency"]),
			P95Latency:    dbutil.Float64FromAny(row["p95_latency"]),
			P99Latency:    dbutil.Float64FromAny(row["p99_latency"]),
		})
	}
	return metrics, nil
}

// GetEndpointTimeSeries returns per-minute time series for endpoint metrics.
func (r *ClickHouseRepository) GetEndpointTimeSeries(ctx context.Context, f MetricFilters) ([]TimeSeriesPoint, error) {
	view := selectEndpointView(f.Start, f.End)
	query := fmt.Sprintf(`
		SELECT minute          AS time_bucket,
		       service_name,
		       operation_name,
		       http_method,
		       countMerge(request_count)       AS request_count,
		       countMerge(error_count)         AS error_count,
		       avgMerge(avg_state)             AS avg_latency,
		       quantileMerge(0.5)(p50_state)   AS p50,
		       quantileMerge(0.95)(p95_state)  AS p95,
		       quantileMerge(0.99)(p99_state)  AS p99
		FROM %s
		WHERE team_id = ? AND minute BETWEEN ? AND ?`, view)
	args := []any{f.TeamUUID, f.Start, f.End}
	if f.ServiceName != "" {
		query += ` AND service_name = ?`
		args = append(args, f.ServiceName)
	}
	query += ` GROUP BY minute, service_name, operation_name, http_method ORDER BY time_bucket ASC, countMerge(request_count) DESC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]TimeSeriesPoint, 0, len(rows))
	for _, row := range rows {
		points = append(points, TimeSeriesPoint{
			Timestamp:     dbutil.TimeFromAny(row["time_bucket"]),
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			HTTPMethod:    dbutil.StringFromAny(row["http_method"]),
			RequestCount:  dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:    dbutil.Float64FromAny(row["avg_latency"]),
			P50:           dbutil.Float64FromAny(row["p50"]),
			P95:           dbutil.Float64FromAny(row["p95"]),
			P99:           dbutil.Float64FromAny(row["p99"]),
		})
	}
	return points, nil
}
