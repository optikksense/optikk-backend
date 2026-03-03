package overview

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// overviewBucketExpr returns a ClickHouse expression for adaptive time bucketing
// over the raw spans table using OpenTelemetry conventions.
func overviewBucketExpr(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return IntervalOneMinute
	case hours <= 24:
		return IntervalFiveMinutes
	case hours <= 168:
		return IntervalSixtyMinutes
	default:
		return IntervalOneDay
	}
}

// Repository encapsulates data access logic for overview dashboards.
type Repository interface {
	GetSummary(teamUUID string, startMs, endMs int64) (Summary, error)
	GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetServices(teamUUID string, startMs, endMs int64) ([]ServiceMetric, error)
	GetEndpointMetrics(teamUUID string, startMs, endMs int64, serviceName string) ([]EndpointMetric, error)
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

func (r *ClickHouseRepository) GetSummary(teamUUID string, startMs, endMs int64) (Summary, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT total_requests,
		       error_count,
		       if(total_requests > 0, error_count*100.0/total_requests, 0) AS error_rate,
		       avg_latency,
		       p95_latency,
		       p99_latency
		FROM (
			SELECT count()                         AS total_requests,
			       countIf(`+ErrorCondition()+`) AS error_count,
			       avg(`+ColDurationMs+`)                AS avg_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(`+ColDurationMs+`)     AS p95_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP99)+`)(`+ColDurationMs+`)     AS p99_latency
			FROM spans
			WHERE `+ColTeamID+` = ? AND `+RootSpanCondition()+` AND `+ColStartTime+` BETWEEN ? AND ?
		)
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return Summary{}, err
	}

	return Summary{
		TotalRequests: dbutil.Int64FromAny(row["total_requests"]),
		ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
		ErrorRate:     dbutil.Float64FromAny(row["error_rate"]),
		AvgLatency:    dbutil.Float64FromAny(row["avg_latency"]),
		P95Latency:    dbutil.Float64FromAny(row["p95_latency"]),
		P99Latency:    dbutil.Float64FromAny(row["p99_latency"]),
	}, nil
}

func (r *ClickHouseRepository) GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	bucket := overviewBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s                          AS time_bucket,
		       count()                         AS request_count,
		       countIf(`+ErrorCondition()+`) AS error_count,
		       avg(`+ColDurationMs+`)                AS avg_latency,
		       quantile(`+fmt.Sprintf("%.1f", QuantileP50)+`)(`+ColDurationMs+`)      AS p50,
		       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(`+ColDurationMs+`)     AS p95,
		       quantile(`+fmt.Sprintf("%.2f", QuantileP99)+`)(`+ColDurationMs+`)     AS p99
		FROM spans
		WHERE `+ColTeamID+` = ? AND `+RootSpanCondition()+` AND `+ColStartTime+` BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND ` + ColServiceName + ` = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY %s ORDER BY time_bucket ASC`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
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

func (r *ClickHouseRepository) GetServices(teamUUID string, startMs, endMs int64) ([]ServiceMetric, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT *
		FROM (
			SELECT `+ColServiceName+`,
			       count()                        AS request_count,
			       countIf(`+ErrorCondition()+`) AS error_count,
			       avg(`+ColDurationMs+`)               AS avg_latency,
			       quantile(`+fmt.Sprintf("%.1f", QuantileP50)+`)(`+ColDurationMs+`)     AS p50_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(`+ColDurationMs+`)    AS p95_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP99)+`)(`+ColDurationMs+`)    AS p99_latency
			FROM spans
			WHERE `+ColTeamID+` = ? AND `+RootSpanCondition()+` AND `+ColStartTime+` BETWEEN ? AND ?
			GROUP BY `+ColServiceName+`
		)
		ORDER BY request_count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetEndpointMetrics(teamUUID string, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	query := `
		SELECT *
		FROM (
			SELECT ` + ColServiceName + `, ` + ColOperationName + `, ` + ColHTTPMethod + `,
			       count()                        AS request_count,
			       countIf(` + ErrorCondition() + `) AS error_count,
			       avg(` + ColDurationMs + `)               AS avg_latency,
			       quantile(` + fmt.Sprintf("%.1f", QuantileP50) + `)(` + ColDurationMs + `)     AS p50_latency,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP95) + `)(` + ColDurationMs + `)    AS p95_latency,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP99) + `)(` + ColDurationMs + `)    AS p99_latency
			FROM spans
			WHERE ` + ColTeamID + ` = ? AND ` + RootSpanCondition() + ` AND ` + ColStartTime + ` BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND ` + ColServiceName + ` = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY ` + ColServiceName + `, ` + ColOperationName + `, ` + ColHTTPMethod + `
		)
		ORDER BY request_count DESC
		LIMIT 100`

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
		SELECT *
		FROM (
			SELECT %s         AS time_bucket,
			       `+ColServiceName+`,
			       `+ColOperationName+`,
			       `+ColHTTPMethod+`,
			       count()                        AS request_count,
			       countIf(`+ErrorCondition()+`) AS error_count,
			       avg(`+ColDurationMs+`)               AS avg_latency,
			       quantile(`+fmt.Sprintf("%.1f", QuantileP50)+`)(`+ColDurationMs+`)     AS p50,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(`+ColDurationMs+`)    AS p95,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP99)+`)(`+ColDurationMs+`)    AS p99
			FROM spans
			WHERE `+ColTeamID+` = ? AND `+RootSpanCondition()+` AND `+ColStartTime+` BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND ` + ColServiceName + ` = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY %s, `+ColServiceName+`, `+ColOperationName+`, `+ColHTTPMethod+`
		)
		ORDER BY time_bucket ASC, request_count DESC`, bucket)

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
