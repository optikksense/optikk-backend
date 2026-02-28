package store

import (
	"context"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/modules/metrics/model"
)

// Repository defines the data access layer for metrics.
type Repository interface {
	GetDashboardOverview(ctx context.Context, teamUUID string, start, end time.Time) ([]map[string]any, []map[string]any, []map[string]any, error)
	GetDashboardServices(ctx context.Context, teamUUID string, start, end time.Time) ([]map[string]any, error)
	GetDashboardServiceDetail(ctx context.Context, teamUUID, serviceName string, start, end time.Time) (map[string]any, error)
	GetServiceMetrics(ctx context.Context, teamUUID string, start, end time.Time) ([]model.ServiceMetric, error)
	GetEndpointMetrics(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.EndpointMetric, error)
	GetMetricsTimeSeries(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.TimeSeriesPoint, error)
	GetMetricsSummary(ctx context.Context, teamUUID string, start, end time.Time) (model.MetricsSummary, error)
	GetServiceTimeSeries(ctx context.Context, teamUUID string, start, end time.Time) ([]model.TimeSeriesPoint, error)
	GetServiceTopologyNodes(ctx context.Context, teamUUID string, start, end time.Time) ([]model.TopologyNode, error)
	GetServiceTopologyEdges(ctx context.Context, teamUUID string, start, end time.Time) ([]model.TopologyEdge, error)
	GetEndpointTimeSeries(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.TimeSeriesPoint, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetDashboardOverview(ctx context.Context, teamUUID string, start, end time.Time) ([]map[string]any, []map[string]any, []map[string]any, error) {
	serviceMetrics, err := dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       countMerge(request_count) AS request_count,
		       countMerge(error_count)   AS error_count,
		       avgMerge(avg_state)       AS avg_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
		GROUP BY service_name
	`, teamUUID, start, end)
	if err != nil {
		return nil, nil, nil, err
	}

	logsData, err := dbutil.QueryMaps(r.db, `
		SELECT timestamp, level, service_name, message, trace_id, span_id
		FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC
		LIMIT 10
	`, teamUUID, start, end)
	if err != nil {
		return nil, nil, nil, err
	}

	tracesData, err := dbutil.QueryMaps(r.db, `
		SELECT trace_id, service_name, operation_name, start_time, duration_ms, status
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
		ORDER BY start_time DESC
		LIMIT 10
	`, teamUUID, start, end)

	return serviceMetrics, logsData, tracesData, err
}

func (r *ClickHouseRepository) GetDashboardServices(ctx context.Context, teamUUID string, start, end time.Time) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       countMerge(request_count) AS request_count,
		       countMerge(error_count)   AS error_count,
		       avgMerge(avg_state)       AS avg_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY countMerge(request_count) DESC
	`, teamUUID, start, end)
}

func (r *ClickHouseRepository) GetDashboardServiceDetail(ctx context.Context, teamUUID, serviceName string, start, end time.Time) (map[string]any, error) {
	return dbutil.QueryMap(r.db, `
		SELECT service_name,
		       countMerge(request_count) AS request_count,
		       countMerge(error_count)   AS error_count,
		       avgMerge(avg_state)       AS avg_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND service_name = ? AND minute BETWEEN ? AND ?
		GROUP BY service_name
		LIMIT 1
	`, teamUUID, serviceName, start, end)
}

func (r *ClickHouseRepository) GetServiceMetrics(ctx context.Context, teamUUID string, start, end time.Time) ([]model.ServiceMetric, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       countMerge(request_count)       AS request_count,
		       countMerge(error_count)         AS error_count,
		       avgMerge(avg_state)             AS avg_latency,
		       quantileMerge(0.5)(p50_state)   AS p50_latency,
		       quantileMerge(0.95)(p95_state)  AS p95_latency,
		       quantileMerge(0.99)(p99_state)  AS p99_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY countMerge(request_count) DESC
	`, teamUUID, start, end)
	if err != nil {
		return nil, err
	}

	metrics := make([]model.ServiceMetric, 0, len(rows))
	for _, row := range rows {
		metrics = append(metrics, model.ServiceMetric{
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
			P50Latency:   dbutil.Float64FromAny(row["p50_latency"]),
			P95Latency:   dbutil.Float64FromAny(row["p95_latency"]),
			P99Latency:   dbutil.Float64FromAny(row["p99_latency"]),
		})
	}
	return metrics, nil
}

func (r *ClickHouseRepository) GetEndpointMetrics(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.EndpointMetric, error) {
	query := `
		SELECT service_name, operation_name, http_method,
		       countMerge(request_count)       AS request_count,
		       countMerge(error_count)         AS error_count,
		       avgMerge(avg_state)             AS avg_latency,
		       quantileMerge(0.5)(p50_state)   AS p50_latency,
		       quantileMerge(0.95)(p95_state)  AS p95_latency,
		       quantileMerge(0.99)(p99_state)  AS p99_latency
		FROM observability.spans_endpoint_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?`
	args := []any{teamUUID, start, end}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, operation_name, http_method ORDER BY countMerge(request_count) DESC LIMIT 100`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	metrics := make([]model.EndpointMetric, 0, len(rows))
	for _, row := range rows {
		metrics = append(metrics, model.EndpointMetric{
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

func (r *ClickHouseRepository) GetMetricsTimeSeries(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.TimeSeriesPoint, error) {
	query := `
		SELECT minute                          AS time_bucket,
		       countMerge(request_count)       AS request_count,
		       countMerge(error_count)         AS error_count,
		       avgMerge(avg_state)             AS avg_latency,
		       quantileMerge(0.5)(p50_state)   AS p50,
		       quantileMerge(0.95)(p95_state)  AS p95,
		       quantileMerge(0.99)(p99_state)  AS p99
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?`
	args := []any{teamUUID, start, end}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY minute ORDER BY time_bucket ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]model.TimeSeriesPoint, 0, len(rows))
	for _, row := range rows {
		points = append(points, model.TimeSeriesPoint{
			Timestamp:    dbutil.TimeFromAny(row["time_bucket"]),
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

func (r *ClickHouseRepository) GetMetricsSummary(ctx context.Context, teamUUID string, start, end time.Time) (model.MetricsSummary, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT countMerge(request_count) AS total_requests,
		       countMerge(error_count)   AS error_count,
		       if(countMerge(request_count) > 0,
		          countMerge(error_count)*100.0/countMerge(request_count), 0) AS error_rate,
		       avgMerge(avg_state)            AS avg_latency,
		       quantileMerge(0.95)(p95_state) AS p95_latency,
		       quantileMerge(0.99)(p99_state) AS p99_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
	`, teamUUID, start, end)
	if err != nil {
		return model.MetricsSummary{}, err
	}

	if len(row) == 0 {
		return model.MetricsSummary{}, nil
	}

	return model.MetricsSummary{
		TotalRequests: dbutil.Int64FromAny(row["total_requests"]),
		ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
		ErrorRate:     dbutil.Float64FromAny(row["error_rate"]),
		AvgLatency:    dbutil.Float64FromAny(row["avg_latency"]),
		P95Latency:    dbutil.Float64FromAny(row["p95_latency"]),
		P99Latency:    dbutil.Float64FromAny(row["p99_latency"]),
	}, nil
}

func (r *ClickHouseRepository) GetServiceTimeSeries(ctx context.Context, teamUUID string, start, end time.Time) ([]model.TimeSeriesPoint, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       minute                    AS timestamp,
		       countMerge(request_count) AS request_count,
		       countMerge(error_count)   AS error_count,
		       avgMerge(avg_state)       AS avg_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
		GROUP BY service_name, minute
		ORDER BY minute ASC, countMerge(request_count) DESC
	`, teamUUID, start, end)
	if err != nil {
		return nil, err
	}

	points := make([]model.TimeSeriesPoint, 0, len(rows))
	for _, row := range rows {
		points = append(points, model.TimeSeriesPoint{
			Timestamp:    dbutil.TimeFromAny(row["timestamp"]),
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
		})
	}
	return points, nil
}

func (r *ClickHouseRepository) GetServiceTopologyNodes(ctx context.Context, teamUUID string, start, end time.Time) ([]model.TopologyNode, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       countMerge(request_count) AS request_count,
		       countMerge(error_count)   AS error_count,
		       avgMerge(avg_state)       AS avg_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY countMerge(request_count) DESC
	`, teamUUID, start, end)
	if err != nil {
		return nil, err
	}

	nodes := make([]model.TopologyNode, 0, len(rows))
	for _, row := range rows {
		reqCount := dbutil.Int64FromAny(row["request_count"])
		errCount := dbutil.Int64FromAny(row["error_count"])
		errRate := 0.0
		if reqCount > 0 {
			errRate = float64(errCount) * 100.0 / float64(reqCount)
		}

		nodes = append(nodes, model.TopologyNode{
			Name:         dbutil.StringFromAny(row["service_name"]),
			RequestCount: reqCount,
			ErrorRate:    errRate,
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
		})
	}
	return nodes, nil
}

// GetServiceTopologyEdges reads from spans_edges_1m — no self-JOIN on raw spans.
func (r *ClickHouseRepository) GetServiceTopologyEdges(ctx context.Context, teamUUID string, start, end time.Time) ([]model.TopologyEdge, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT parent_service_name                                              AS source,
		       service_name                                                     AS target,
		       countMerge(call_count)                                          AS call_count,
		       avgMerge(avg_latency_state)                                     AS avg_latency,
		       if(countMerge(call_count) > 0,
		          countMerge(error_count)*100.0/countMerge(call_count), 0)    AS error_rate
		FROM observability.spans_edges_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
		GROUP BY parent_service_name, service_name
		ORDER BY countMerge(call_count) DESC
		LIMIT 100
	`, teamUUID, start, end)
	if err != nil {
		return nil, err
	}

	edges := make([]model.TopologyEdge, 0, len(rows))
	for _, row := range rows {
		edges = append(edges, model.TopologyEdge{
			Source:     dbutil.StringFromAny(row["source"]),
			Target:     dbutil.StringFromAny(row["target"]),
			CallCount:  dbutil.Int64FromAny(row["call_count"]),
			AvgLatency: dbutil.Float64FromAny(row["avg_latency"]),
			ErrorRate:  dbutil.Float64FromAny(row["error_rate"]),
		})
	}
	return edges, nil
}

func (r *ClickHouseRepository) GetEndpointTimeSeries(ctx context.Context, teamUUID string, start, end time.Time, serviceName string) ([]model.TimeSeriesPoint, error) {
	query := `
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
		FROM observability.spans_endpoint_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?`
	args := []any{teamUUID, start, end}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY minute, service_name, operation_name, http_method ORDER BY time_bucket ASC, countMerge(request_count) DESC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]model.TimeSeriesPoint, 0, len(rows))
	for _, row := range rows {
		points = append(points, model.TimeSeriesPoint{
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
