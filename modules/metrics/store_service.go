package metrics

import (
	"context"
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// GetServiceMetrics returns aggregate metrics per service. This replaces the
// old GetDashboardServices, GetServiceTopologyNodes, and GetServiceMetrics.
func (r *ClickHouseRepository) GetServiceMetrics(ctx context.Context, f MetricFilters) ([]ServiceMetric, error) {
	view := selectServiceView(f.Start, f.End)
	query := fmt.Sprintf(`
		SELECT service_name,
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
	query += ` GROUP BY service_name ORDER BY countMerge(request_count) DESC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	metrics := make([]ServiceMetric, 0, len(rows))
	for _, row := range rows {
		reqCount := dbutil.Int64FromAny(row["request_count"])
		errCount := dbutil.Int64FromAny(row["error_count"])
		rate := errorRate(reqCount, errCount)
		metrics = append(metrics, ServiceMetric{
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: reqCount,
			ErrorCount:   errCount,
			ErrorRate:    rate,
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
			P50Latency:   dbutil.Float64FromAny(row["p50_latency"]),
			P95Latency:   dbutil.Float64FromAny(row["p95_latency"]),
			P99Latency:   dbutil.Float64FromAny(row["p99_latency"]),
			Status:       serviceStatus(rate),
		})
	}
	return metrics, nil
}

// GetServiceTimeSeries returns time-bucketed time series for service metrics.
// Uses autoStep to pick a bucket size (~30 points) so that wide windows
// (e.g. 30d) don't return 43,200 raw minute rows.
func (r *ClickHouseRepository) GetServiceTimeSeries(ctx context.Context, f MetricFilters) ([]TimeSeriesPoint, error) {
	view := selectServiceView(f.Start, f.End)
	step := autoStep(f.Start, f.End)
	bucket := metricBucketExpr(step)
	query := fmt.Sprintf(`
		SELECT %s                              AS time_bucket,
		       service_name,
		       countMerge(request_count)       AS request_count,
		       countMerge(error_count)         AS error_count,
		       avgMerge(avg_state)             AS avg_latency,
		       quantileMerge(0.5)(p50_state)   AS p50,
		       quantileMerge(0.95)(p95_state)  AS p95,
		       quantileMerge(0.99)(p99_state)  AS p99
		FROM %s
		WHERE team_id = ? AND minute BETWEEN ? AND ?`, bucket, view)
	args := []any{f.TeamUUID, f.Start, f.End}
	if f.ServiceName != "" {
		query += ` AND service_name = ?`
		args = append(args, f.ServiceName)
	}
	query += fmt.Sprintf(` GROUP BY service_name, %s ORDER BY time_bucket ASC, countMerge(request_count) DESC`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]TimeSeriesPoint, 0, len(rows))
	for _, row := range rows {
		points = append(points, TimeSeriesPoint{
			Timestamp:    dbutil.TimeFromAny(row["time_bucket"]),
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
			P50:          dbutil.Float64FromAny(row["p50"]),
			P95:          dbutil.Float64FromAny(row["p95"]),
			P99:          dbutil.Float64FromAny(row["p99"]),
		})
	}
	return points, nil
}

// GetMetricsSummary returns a high-level aggregate summary across all services.
func (r *ClickHouseRepository) GetMetricsSummary(ctx context.Context, f MetricFilters) (MetricsSummary, error) {
	view := selectServiceView(f.Start, f.End)
	row, err := dbutil.QueryMap(r.db, fmt.Sprintf(`
		SELECT countMerge(request_count) AS total_requests,
		       countMerge(error_count)   AS error_count,
		       if(countMerge(request_count) > 0,
		          countMerge(error_count)*100.0/countMerge(request_count), 0) AS error_rate,
		       avgMerge(avg_state)            AS avg_latency,
		       quantileMerge(0.95)(p95_state) AS p95_latency,
		       quantileMerge(0.99)(p99_state) AS p99_latency
		FROM %s
		WHERE team_id = ? AND minute BETWEEN ? AND ?
	`, view), f.TeamUUID, f.Start, f.End)
	if err != nil {
		return MetricsSummary{}, err
	}

	if len(row) == 0 {
		return MetricsSummary{}, nil
	}

	return MetricsSummary{
		TotalRequests: dbutil.Int64FromAny(row["total_requests"]),
		ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
		ErrorRate:     dbutil.Float64FromAny(row["error_rate"]),
		AvgLatency:    dbutil.Float64FromAny(row["avg_latency"]),
		P95Latency:    dbutil.Float64FromAny(row["p95_latency"]),
		P99Latency:    dbutil.Float64FromAny(row["p99_latency"]),
	}, nil
}
