package servicepage

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// serviceBucketExpr returns a ClickHouse expression for adaptive time bucketing
// over the spans table using s.timestamp.
func serviceBucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
}

// Repository encapsulates data access logic for the services overview page.
type Repository interface {
	GetTotalServices(teamID int64, startMs, endMs int64) (int64, error)
	GetHealthyServices(teamID int64, startMs, endMs int64) (int64, error)
	GetDegradedServices(teamID int64, startMs, endMs int64) (int64, error)
	GetUnhealthyServices(teamID int64, startMs, endMs int64) (int64, error)
	GetServiceMetrics(teamID int64, startMs, endMs int64) ([]ServiceMetric, error)
	GetServiceTimeSeries(teamID int64, startMs, endMs int64) ([]TimeSeriesPoint, error)
	GetServiceEndpoints(teamID int64, startMs, endMs int64, serviceName string) ([]EndpointMetric, error)
}

// ClickHouseRepository encapsulates services overview data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new services overview repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetTotalServices(teamID int64, startMs, endMs int64) (int64, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as count
		FROM (
			SELECT s.service_name AS service_name
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
			GROUP BY s.service_name
		)
	`, teamID, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return 0, err
	}
	return dbutil.Int64FromAny(row["count"]), nil
}

func (r *ClickHouseRepository) GetHealthyServices(teamID int64, startMs, endMs int64) (int64, error) {
	return r.countServicesByErrorRate(teamID, startMs, endMs, "error_rate <= ?", HealthyMaxErrorRate)
}

func (r *ClickHouseRepository) GetDegradedServices(teamID int64, startMs, endMs int64) (int64, error) {
	return r.countServicesByErrorRate(teamID, startMs, endMs, "error_rate > ? AND error_rate <= ?", HealthyMaxErrorRate, DegradedMaxErrorRate)
}

func (r *ClickHouseRepository) GetUnhealthyServices(teamID int64, startMs, endMs int64) (int64, error) {
	return r.countServicesByErrorRate(teamID, startMs, endMs, "error_rate > ?", DegradedMaxErrorRate)
}

func (r *ClickHouseRepository) GetServiceMetrics(teamID int64, startMs, endMs int64) ([]ServiceMetric, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT service_name, request_count, error_count, avg_latency, p50_latency, p95_latency, p99_latency
		FROM (
			SELECT s.service_name AS service_name,
			       count()                                                                      AS request_count,
			       countIf(`+ErrorCondition()+`)                                               AS error_count,
			       avg(s.duration_nano / 1000000.0)                                            AS avg_latency,
			       quantile(`+fmt.Sprintf("%.1f", QuantileP50)+`)(s.duration_nano / 1000000.0) AS p50_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) AS p95_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP99)+`)(s.duration_nano / 1000000.0) AS p99_latency
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
			GROUP BY s.service_name
		)
		ORDER BY request_count DESC
	`, teamID, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	metrics := make([]ServiceMetric, len(rows))
	for i, row := range rows {
		metrics[i] = ServiceMetric{
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
			P50Latency:   dbutil.Float64FromAny(row["p50_latency"]),
			P95Latency:   dbutil.Float64FromAny(row["p95_latency"]),
			P99Latency:   dbutil.Float64FromAny(row["p99_latency"]),
		}
	}
	return metrics, nil
}

func (r *ClickHouseRepository) GetServiceTimeSeries(teamID int64, startMs, endMs int64) ([]TimeSeriesPoint, error) {
	bucket := serviceBucketExpr(startMs, endMs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT service_name, timestamp, request_count, error_count, avg_latency
		FROM (
			SELECT s.service_name AS service_name,
			       %s AS timestamp,
			       count()                          AS request_count,
			       countIf(`+ErrorCondition()+`)    AS error_count,
			       avg(s.duration_nano / 1000000.0) AS avg_latency
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
			GROUP BY s.service_name, %s
		)
		ORDER BY timestamp ASC, request_count DESC
		LIMIT 10000
	`, bucket, bucket), teamID, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
			Timestamp:    dbutil.TimeFromAny(row["timestamp"]),
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
		}
	}
	return points, nil
}

func (r *ClickHouseRepository) GetServiceEndpoints(teamID int64, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT service_name, operation_name, http_method, request_count, error_count, avg_latency, p50_latency, p95_latency, p99_latency
		FROM (
			SELECT s.service_name AS service_name, s.name AS operation_name, s.http_method AS http_method,
			       count()                                                                      AS request_count,
			       countIf(`+ErrorCondition()+`)                                               AS error_count,
			       avg(s.duration_nano / 1000000.0)                                            AS avg_latency,
			       quantile(`+fmt.Sprintf("%.1f", QuantileP50)+`)(s.duration_nano / 1000000.0) AS p50_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) AS p95_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP99)+`)(s.duration_nano / 1000000.0) AS p99_latency
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ? AND s.service_name = ?
			GROUP BY s.service_name, s.name, s.http_method
		)
		ORDER BY request_count DESC
		LIMIT 100
	`, teamID, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), serviceName)
	if err != nil {
		return nil, err
	}

	endpoints := make([]EndpointMetric, len(rows))
	for i, row := range rows {
		endpoints[i] = EndpointMetric{
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
	return endpoints, nil
}

func (r *ClickHouseRepository) countServicesByErrorRate(teamID int64, startMs, endMs int64, havingClause string, args ...any) (int64, error) {
	queryArgs := []any{teamID, uint64(startMs / 1000), uint64(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	queryArgs = append(queryArgs, args...)

	row, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as count
		FROM (
			SELECT s.service_name AS service_name,
			       if(count() > 0,
			          countIf(`+ErrorCondition()+`)*100.0/count(), 0) as error_rate
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
			GROUP BY s.service_name
			HAVING `+havingClause+`
		)
	`, queryArgs...)
	if err != nil {
		return 0, err
	}
	return dbutil.Int64FromAny(row["count"]), nil
}
