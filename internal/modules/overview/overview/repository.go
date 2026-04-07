package overview

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

const serviceNameFilter = " AND s.service_name = @serviceName"

func overviewBucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
}

type Repository interface {
	GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]requestRateRow, error)
	GetErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorRateRow, error)
	GetP95Latency(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]p95LatencyRow, error)
	GetServices(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceMetricRow, error)
	GetTopEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]endpointMetricRow, error)
	GetEndpointTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]timeSeriesRow, error)
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (serviceMetricRow, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// requestRateRow is the DTO for GetRequestRate.
type requestRateRow struct {
	Timestamp    time.Time `ch:"time_bucket"`
	ServiceName  string    `ch:"service_name"`
	RequestCount int64     `ch:"request_count"`
}

func (r *ClickHouseRepository) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]requestRateRow, error) {
	bucket := overviewBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT time_bucket, service_name, request_count
		FROM (
			SELECT %s AS time_bucket,
			       s.service_name AS service_name,
			       toInt64(count()) AS request_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(` GROUP BY %s, s.service_name
		)
		ORDER BY time_bucket ASC, service_name ASC
		LIMIT 10000`, bucket)

	var rows []requestRateRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// errorRateRow is the DTO for GetErrorRate.
type errorRateRow struct {
	Timestamp    time.Time `ch:"time_bucket"`
	ServiceName  string    `ch:"service_name"`
	RequestCount int64     `ch:"request_count"`
	ErrorCount   int64     `ch:"error_count"`
	ErrorRate    float64   `ch:"error_rate"`
}

func (r *ClickHouseRepository) GetErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]errorRateRow, error) {
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
			       toInt64(count()) AS request_count,
			       toInt64(countIf(`+ErrorCondition()+`)) AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(` GROUP BY %s, s.service_name
		)
		ORDER BY time_bucket ASC, service_name ASC
		LIMIT 10000`, bucket)

	var rows []errorRateRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// p95LatencyRow is the DTO for GetP95Latency.
type p95LatencyRow struct {
	Timestamp   time.Time `ch:"time_bucket"`
	ServiceName string    `ch:"service_name"`
	P95         float64   `ch:"p95"`
}

func (r *ClickHouseRepository) GetP95Latency(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]p95LatencyRow, error) {
	bucket := overviewBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT time_bucket, service_name, p95
		FROM (
			SELECT %s AS time_bucket,
			       s.service_name AS service_name,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) AS p95
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(` GROUP BY %s, s.service_name
		)
		ORDER BY time_bucket ASC, service_name ASC
		LIMIT 10000`, bucket)

	var rows []p95LatencyRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// serviceMetricRow is the DTO for GetServices.
type serviceMetricRow struct {
	ServiceName  string  `ch:"service_name"`
	RequestCount int64   `ch:"request_count"`
	ErrorCount   int64   `ch:"error_count"`
	AvgLatency   float64 `ch:"avg_latency"`
	P50Latency   float64 `ch:"p50_latency"`
	P95Latency   float64 `ch:"p95_latency"`
	P99Latency   float64 `ch:"p99_latency"`
}

func (r *ClickHouseRepository) GetServices(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceMetricRow, error) {
	query := `
		SELECT service_name, request_count, error_count, avg_latency, p50_latency, p95_latency, p99_latency
		FROM (
			SELECT s.service_name AS service_name,
			       toInt64(count())                                                             AS request_count,
			       toInt64(countIf(` + ErrorCondition() + `))                                   AS error_count,
			       avg(s.duration_nano / 1000000.0)                                            AS avg_latency,
			       quantile(` + fmt.Sprintf("%.1f", QuantileP50) + `)(s.duration_nano / 1000000.0) AS p50_latency,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP95) + `)(s.duration_nano / 1000000.0) AS p95_latency,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP99) + `)(s.duration_nano / 1000000.0) AS p99_latency
			FROM observability.spans s
			WHERE s.team_id = @teamID AND ` + RootSpanCondition() + ` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name
		)
		ORDER BY request_count DESC
	`

	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	var rows []serviceMetricRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// endpointMetricRow is the DTO for GetTopEndpoints.
type endpointMetricRow struct {
	ServiceName   string  `ch:"service_name"`
	OperationName string  `ch:"operation_name"`
	EndpointName  string  `ch:"endpoint_name"`
	HTTPMethod    string  `ch:"http_method"`
	RequestCount  int64   `ch:"request_count"`
	ErrorCount    int64   `ch:"error_count"`
	AvgLatency    float64 `ch:"avg_latency"`
	P50Latency    float64 `ch:"p50_latency"`
	P95Latency    float64 `ch:"p95_latency"`
	P99Latency    float64 `ch:"p99_latency"`
}

func (r *ClickHouseRepository) GetTopEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]endpointMetricRow, error) {
	query := `
		SELECT service_name, operation_name, endpoint_name, http_method, request_count, error_count, avg_latency, p50_latency, p95_latency, p99_latency
		FROM (
			SELECT s.service_name AS service_name,
			       s.name AS operation_name,
			       coalesce(nullIf(s.mat_http_route, ''), nullIf(s.mat_http_target, ''), s.name) AS endpoint_name,
			       s.http_method,
			       toInt64(count()) AS request_count,
			       toInt64(countIf(` + ErrorCondition() + `)) AS error_count,
			       avg(s.duration_nano / 1000000.0) AS avg_latency,
			       quantile(` + fmt.Sprintf("%.1f", QuantileP50) + `)(s.duration_nano / 1000000.0) AS p50_latency,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP95) + `)(s.duration_nano / 1000000.0) AS p95_latency,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP99) + `)(s.duration_nano / 1000000.0) AS p99_latency
			FROM observability.spans s
			WHERE s.team_id = @teamID AND ` + RootSpanCondition() + ` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end AND
				tuple(s.service_name, s.name, s.http_method, coalesce(nullIf(s.mat_http_route, ''), nullIf(s.mat_http_target, ''), s.name)) IN (
					SELECT service_name, name, http_method, coalesce(nullIf(mat_http_route, ''), nullIf(mat_http_target, ''), name) AS endpoint_name
					FROM observability.spans s
					WHERE s.team_id = @teamID AND ` + RootSpanCondition() + ` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
					GROUP BY service_name, name, http_method, endpoint_name
					ORDER BY count() DESC
					LIMIT 100
				)`
	if serviceName != "" {
		query += serviceNameFilter
	}
	query += `
			GROUP BY s.service_name, s.name, endpoint_name, s.http_method
		)
		ORDER BY request_count DESC`

	var rows []endpointMetricRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

// timeSeriesRow is the DTO for GetEndpointTimeSeries.
type timeSeriesRow struct {
	Timestamp     time.Time `ch:"time_bucket"`
	ServiceName   string    `ch:"service_name"`
	OperationName string    `ch:"operation_name"`
	HTTPMethod    string    `ch:"http_method"`
	RequestCount  int64     `ch:"request_count"`
	ErrorCount    int64     `ch:"error_count"`
	AvgLatency    float64   `ch:"avg_latency"`
	P50           float64   `ch:"p50"`
	P95           float64   `ch:"p95"`
	P99           float64   `ch:"p99"`
}

func (r *ClickHouseRepository) GetEndpointTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]timeSeriesRow, error) {
	bucket := overviewBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT time_bucket, service_name, operation_name, http_method, request_count, error_count, avg_latency, p50, p95, p99
		FROM (
			SELECT %s        AS time_bucket,
			       s.service_name AS service_name,
			       s.name AS operation_name,
			       s.http_method AS http_method,
			       toInt64(count())                                                             AS request_count,
			       toInt64(countIf(`+ErrorCondition()+`))                                       AS error_count,
			       avg(s.duration_nano / 1000000.0)                                            AS avg_latency,
			       quantile(`+fmt.Sprintf("%.1f", QuantileP50)+`)(s.duration_nano / 1000000.0) AS p50,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) AS p95,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP99)+`)(s.duration_nano / 1000000.0) AS p99
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += fmt.Sprintf(` GROUP BY %s, s.service_name, s.name, s.http_method
		)
		ORDER BY time_bucket ASC, request_count DESC
		LIMIT 10000`, bucket)

	var rows []timeSeriesRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}

	return rows, nil
}

func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (serviceMetricRow, error) {
	query := `
		SELECT '' AS service_name,
		       toInt64(count())                                                             AS request_count,
		       toInt64(countIf(` + ErrorCondition() + `))                                   AS error_count,
		       avg(s.duration_nano / 1000000.0)                                            AS avg_latency,
		       quantile(` + fmt.Sprintf("%.1f", QuantileP50) + `)(s.duration_nano / 1000000.0) AS p50_latency,
		       quantile(` + fmt.Sprintf("%.2f", QuantileP95) + `)(s.duration_nano / 1000000.0) AS p95_latency,
		       quantile(` + fmt.Sprintf("%.2f", QuantileP99) + `)(s.duration_nano / 1000000.0) AS p99_latency
		FROM observability.spans s
		WHERE s.team_id = @teamID AND ` + RootSpanCondition() + ` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
	`

	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	var row serviceMetricRow
	if err := r.db.QueryRow(ctx, &row, query, args...); err != nil {
		return serviceMetricRow{}, err
	}

	return row, nil
}
