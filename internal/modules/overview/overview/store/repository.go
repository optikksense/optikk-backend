package store

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/overview/overview/model"
)

// Repository encapsulates data access logic for overview dashboards.
type Repository interface {
	GetSummary(teamUUID string, startMs, endMs int64) (model.Summary, error)
	GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error)
	GetServices(teamUUID string, startMs, endMs int64) ([]model.ServiceMetric, error)
	GetEndpointMetrics(teamUUID string, startMs, endMs int64, serviceName string) ([]model.EndpointMetric, error)
	GetEndpointTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error)
}

// ClickHouseRepository encapsulates overview data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new overview repository.
func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSummary(teamUUID string, startMs, endMs int64) (model.Summary, error) {
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
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.Summary{}, err
	}

	return model.Summary{
		TotalRequests: dbutil.Int64FromAny(row["total_requests"]),
		ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
		ErrorRate:     dbutil.Float64FromAny(row["error_rate"]),
		AvgLatency:    dbutil.Float64FromAny(row["avg_latency"]),
		P95Latency:    dbutil.Float64FromAny(row["p95_latency"]),
		P99Latency:    dbutil.Float64FromAny(row["p99_latency"]),
	}, nil
}

func (r *ClickHouseRepository) GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error) {
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
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY minute ORDER BY time_bucket ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]model.TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = model.TimeSeriesPoint{
			Timestamp:    dbutil.TimeFromAny(row["time_bucket"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
			P50:          dbutil.Float64FromAny(row["p50"]),
			P95:          dbutil.Float64FromAny(row["p95"]),
			P99:          dbutil.Float64FromAny(row["p99"]),
		}
	}
	return points, nil
}

func (r *ClickHouseRepository) GetServices(teamUUID string, startMs, endMs int64) ([]model.ServiceMetric, error) {
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
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	services := make([]model.ServiceMetric, len(rows))
	for i, row := range rows {
		services[i] = model.ServiceMetric{
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
			P50Latency:   dbutil.Float64FromAny(row["p50_latency"]),
			P95Latency:   dbutil.Float64FromAny(row["p95_latency"]),
			P99Latency:   dbutil.Float64FromAny(row["p99_latency"]),
		}
	}
	return services, nil
}

func (r *ClickHouseRepository) GetEndpointMetrics(teamUUID string, startMs, endMs int64, serviceName string) ([]model.EndpointMetric, error) {
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
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, operation_name, http_method ORDER BY countMerge(request_count) DESC LIMIT 100`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	metrics := make([]model.EndpointMetric, len(rows))
	for i, row := range rows {
		metrics[i] = model.EndpointMetric{
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			HTTPMethod:    dbutil.StringFromAny(row["http_method"]),
			RequestCount:  dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:    dbutil.Float64FromAny(row["avg_latency"]),
			P50Latency:    dbutil.Float64FromAny(row["p50_latency"]),
			P95Latency:    dbutil.Float64FromAny(row["p95_latency"]),
			P99Latency:    dbutil.Float64FromAny(row["p99_latency"]),
		}
	}
	return metrics, nil
}

func (r *ClickHouseRepository) GetEndpointTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error) {
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
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY minute, service_name, operation_name, http_method ORDER BY time_bucket ASC, countMerge(request_count) DESC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]model.TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = model.TimeSeriesPoint{
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
		}
	}
	return points, nil
}
