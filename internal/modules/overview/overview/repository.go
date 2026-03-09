package overview

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// overviewBucketExpr returns a ClickHouse expression for adaptive time bucketing
// over the raw spans table using s.timestamp.
func overviewBucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
}

// Repository encapsulates data access logic for overview dashboards.
type Repository interface {
	GetRequestRate(teamUUID string, startMs, endMs int64, serviceName string) ([]RequestRatePoint, error)
	GetErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]ErrorRatePoint, error)
	GetP95Latency(teamUUID string, startMs, endMs int64, serviceName string) ([]P95LatencyPoint, error)
	GetServices(teamUUID string, startMs, endMs int64) ([]ServiceMetric, error)
	GetTopEndpoints(teamUUID string, startMs, endMs int64, serviceName string) ([]EndpointMetric, error)
	GetEndpointTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
}

// ClickHouseRepository encapsulates overview data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new overview repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetRequestRate(teamUUID string, startMs, endMs int64, serviceName string) ([]RequestRatePoint, error) {
	bucket := overviewBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT time_bucket, service_name, request_count
		FROM (
			SELECT %s AS time_bucket,
			       s.service_name AS service_name,
			       count() AS request_count
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY %s, s.service_name
		)
		ORDER BY time_bucket ASC, service_name ASC
		LIMIT 10000`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]RequestRatePoint, len(rows))
	for i, row := range rows {
		points[i] = RequestRatePoint{
			Timestamp:    dbutil.TimeFromAny(row["time_bucket"]),
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
		}
	}
	return points, nil
}

func (r *ClickHouseRepository) GetErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]ErrorRatePoint, error) {
	bucket := overviewBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT time_bucket,
		       service_name,
		       request_count,
		       error_count,
		       if(request_count > 0, error_count*100.0/request_count, 0) AS error_rate
		FROM (
			SELECT %s AS time_bucket,
			       s.service_name AS service_name,
			       count() AS request_count,
			       countIf(`+ErrorCondition()+`) AS error_count
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY %s, s.service_name
		)
		ORDER BY time_bucket ASC, service_name ASC
		LIMIT 10000`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]ErrorRatePoint, len(rows))
	for i, row := range rows {
		points[i] = ErrorRatePoint{
			Timestamp:    dbutil.TimeFromAny(row["time_bucket"]),
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			ErrorRate:    dbutil.Float64FromAny(row["error_rate"]),
		}
	}
	return points, nil
}

func (r *ClickHouseRepository) GetP95Latency(teamUUID string, startMs, endMs int64, serviceName string) ([]P95LatencyPoint, error) {
	bucket := overviewBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT time_bucket, service_name, p95
		FROM (
			SELECT %s AS time_bucket,
			       s.service_name AS service_name,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) AS p95
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY %s, s.service_name
		)
		ORDER BY time_bucket ASC, service_name ASC
		LIMIT 10000`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]P95LatencyPoint, len(rows))
	for i, row := range rows {
		points[i] = P95LatencyPoint{
			Timestamp:   dbutil.TimeFromAny(row["time_bucket"]),
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			P95:         dbutil.Float64FromAny(row["p95"]),
		}
	}
	return points, nil
}

func (r *ClickHouseRepository) GetServices(teamUUID string, startMs, endMs int64) ([]ServiceMetric, error) {
	query := `
		SELECT service_name, request_count, error_count, avg_latency, p50_latency, p95_latency, p99_latency
		FROM (
			SELECT s.service_name AS service_name,
			       count()                                                                      AS request_count,
			       countIf(` + ErrorCondition() + `)                                               AS error_count,
			       avg(s.duration_nano / 1000000.0)                                            AS avg_latency,
			       quantile(` + fmt.Sprintf("%.1f", QuantileP50) + `)(s.duration_nano / 1000000.0) AS p50_latency,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP95) + `)(s.duration_nano / 1000000.0) AS p95_latency,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP99) + `)(s.duration_nano / 1000000.0) AS p99_latency
			FROM observability.spans s
			WHERE s.team_id = ? AND ` + RootSpanCondition() + ` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
			GROUP BY s.service_name
		)
		ORDER BY request_count DESC
	`
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	services := make([]ServiceMetric, len(rows))
	for i, row := range rows {
		services[i] = ServiceMetric{
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

func (r *ClickHouseRepository) GetTopEndpoints(teamUUID string, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	query := `
		SELECT service_name, operation_name, http_method, request_count, error_count, avg_latency, p50_latency, p95_latency, p99_latency
		FROM (
			SELECT service_name, name AS operation_name, http_method,
			       count() AS request_count,
			       countIf(` + ErrorCondition() + `) AS error_count,
			       avg(duration_nano / 1000000.0) AS avg_latency,
			       quantile(` + fmt.Sprintf("%.1f", QuantileP50) + `)(duration_nano / 1000000.0) AS p50_latency,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP95) + `)(duration_nano / 1000000.0) AS p95_latency,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP99) + `)(duration_nano / 1000000.0) AS p99_latency
			FROM observability.spans
			WHERE team_id = ? AND ` + RootSpanCondition() + ` AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ? AND
				tuple(service_name, name, http_method) IN (
					SELECT service_name, name, http_method
					FROM observability.spans
					WHERE team_id = ? AND ` + RootSpanCondition() + ` AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?`
	args := []any{teamUUID, uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), teamUUID, uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += `
					GROUP BY service_name, name, http_method
					ORDER BY count() DESC
					LIMIT 100
				)`
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += `
			GROUP BY service_name, name, http_method
		)
		ORDER BY request_count DESC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	metrics := make([]EndpointMetric, len(rows))
	for i, row := range rows {
		metrics[i] = EndpointMetric{
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

func (r *ClickHouseRepository) GetEndpointTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	bucket := overviewBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT time_bucket, service_name, operation_name, http_method, request_count, error_count, avg_latency, p50, p95, p99
		FROM (
			SELECT %s        AS time_bucket,
			       s.service_name AS service_name,
			       s.name AS operation_name,
			       s.http_method AS http_method,
			       count()                                                                      AS request_count,
			       countIf(`+ErrorCondition()+`)                                               AS error_count,
			       avg(s.duration_nano / 1000000.0)                                            AS avg_latency,
			       quantile(`+fmt.Sprintf("%.1f", QuantileP50)+`)(s.duration_nano / 1000000.0) AS p50,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) AS p95,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP99)+`)(s.duration_nano / 1000000.0) AS p99
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY %s, s.service_name, s.name, s.http_method
		)
		ORDER BY time_bucket ASC, request_count DESC
		LIMIT 10000`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
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
