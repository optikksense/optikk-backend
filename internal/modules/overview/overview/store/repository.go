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
		SELECT COUNT(*) as total_requests,
		       sum(if(status='ERROR' OR http_status_code >= 400, 1, 0)) as error_count,
		       if(COUNT(*) > 0, sum(if(status='ERROR' OR http_status_code >= 400, 1, 0))*100.0/COUNT(*), 0) as error_rate,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       quantile(0.99)(duration_ms) as p99_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
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
		SELECT toStartOfMinute(start_time) as time_bucket,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR' OR http_status_code >= 400, 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.5)(duration_ms) as p50,
		       quantile(0.95)(duration_ms) as p95,
		       quantile(0.99)(duration_ms) as p99
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY time_bucket ORDER BY time_bucket ASC`

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
		       COUNT(*) as request_count,
		       sum(if(status='ERROR' OR http_status_code >= 400, 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.5)(duration_ms) as p50_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       quantile(0.99)(duration_ms) as p99_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY request_count DESC
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
		       COUNT(*) as request_count,
		       sum(if(status='ERROR' OR http_status_code >= 400, 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.5)(duration_ms) as p50_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       quantile(0.99)(duration_ms) as p99_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, operation_name, http_method ORDER BY request_count DESC LIMIT 100`

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
		SELECT toStartOfMinute(start_time) as time_bucket,
		       service_name,
		       operation_name,
		       http_method,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR' OR http_status_code >= 400, 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.5)(duration_ms) as p50,
		       quantile(0.95)(duration_ms) as p95,
		       quantile(0.99)(duration_ms) as p99
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY time_bucket, service_name, operation_name, http_method ORDER BY time_bucket ASC, request_count DESC`

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
