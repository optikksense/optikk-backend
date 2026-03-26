package servicepage

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

func serviceBucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
}

type Repository interface {
	GetTotalServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error)
	GetHealthyServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error)
	GetDegradedServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error)
	GetUnhealthyServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error)
	GetServiceMetrics(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceMetricDTO, error)
	GetServiceTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeSeriesPointDTO, error)
	GetServiceEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]endpointMetricDTO, error)
	GetServiceHealth(ctx context.Context, teamID, startMs, endMs int64) ([]serviceHealthRow, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetTotalServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error) {
	var row serviceCountRow
	err := r.db.QueryRow(ctx, &row, `
		SELECT toInt64(COUNT(*)) as count
		FROM (
			SELECT s.service_name AS service_name
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name
		)
	`, dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	return row.Count, err
}

func (r *ClickHouseRepository) GetHealthyServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error) {
	return r.countServicesByErrorRate(ctx, teamID, startMs, endMs, "error_rate <= @healthyMax", clickhouse.Named("healthyMax", HealthyMaxErrorRate))
}

func (r *ClickHouseRepository) GetDegradedServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error) {
	return r.countServicesByErrorRate(ctx, teamID, startMs, endMs, "error_rate > @healthyMax AND error_rate <= @degradedMax",
		clickhouse.Named("healthyMax", HealthyMaxErrorRate),
		clickhouse.Named("degradedMax", DegradedMaxErrorRate))
}

func (r *ClickHouseRepository) GetUnhealthyServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error) {
	return r.countServicesByErrorRate(ctx, teamID, startMs, endMs, "error_rate > @degradedMax", clickhouse.Named("degradedMax", DegradedMaxErrorRate))
}

func (r *ClickHouseRepository) GetServiceMetrics(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceMetricDTO, error) {
	var rows []serviceMetricDTO
	err := r.db.Select(ctx, &rows, `
		SELECT service_name, request_count, error_count, avg_latency, p50_latency, p95_latency, p99_latency
		FROM (
			SELECT s.service_name AS service_name,
			       toInt64(count())                                                             AS request_count,
			       toInt64(countIf(`+ErrorCondition()+`))                                       AS error_count,
			       avg(s.duration_nano / 1000000.0)                                            AS avg_latency,
			       quantile(`+fmt.Sprintf("%.1f", QuantileP50)+`)(s.duration_nano / 1000000.0) AS p50_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) AS p95_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP99)+`)(s.duration_nano / 1000000.0) AS p99_latency
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name
		)
		ORDER BY request_count DESC
	`, dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) GetServiceTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeSeriesPointDTO, error) {
	bucket := serviceBucketExpr(startMs, endMs)
	var rows []timeSeriesPointDTO
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT service_name, timestamp, request_count, error_count, avg_latency
		FROM (
			SELECT s.service_name AS service_name,
			       %s AS timestamp,
			       toInt64(count())                 AS request_count,
			       toInt64(countIf(`+ErrorCondition()+`)) AS error_count,
			       avg(s.duration_nano / 1000000.0) AS avg_latency
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name, %s
		)
		ORDER BY timestamp ASC, request_count DESC
		LIMIT 10000
	`, bucket, bucket), dbutil.SpanBaseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) GetServiceEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]endpointMetricDTO, error) {
	var rows []endpointMetricDTO
	params := append(dbutil.SpanBaseParams(teamID, startMs, endMs), clickhouse.Named("serviceName", serviceName))
	err := r.db.Select(ctx, &rows, `
		SELECT service_name, operation_name, http_method, request_count, error_count, avg_latency, p50_latency, p95_latency, p99_latency
		FROM (
			SELECT s.service_name AS service_name, s.name AS operation_name, s.http_method AS http_method,
			       toInt64(count())                                                             AS request_count,
			       toInt64(countIf(`+ErrorCondition()+`))                                       AS error_count,
			       avg(s.duration_nano / 1000000.0)                                            AS avg_latency,
			       quantile(`+fmt.Sprintf("%.1f", QuantileP50)+`)(s.duration_nano / 1000000.0) AS p50_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) AS p95_latency,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP99)+`)(s.duration_nano / 1000000.0) AS p99_latency
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end AND s.service_name = @serviceName
			GROUP BY s.service_name, s.name, s.http_method
		)
		ORDER BY request_count DESC
		LIMIT 100
	`, params...)
	return rows, err
}

func (r *ClickHouseRepository) countServicesByErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, havingClause string, extraArgs ...any) (int64, error) {
	queryArgs := append(dbutil.SpanBaseParams(teamID, startMs, endMs), extraArgs...)

	var row serviceCountRow
	err := r.db.QueryRow(ctx, &row, `
		SELECT toInt64(COUNT(*)) as count
		FROM (
			SELECT s.service_name AS service_name,
			       if(count() > 0,
			          countIf(`+ErrorCondition()+`)*100.0/count(), 0) as error_rate
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name
			HAVING `+havingClause+`
		)
	`, queryArgs...)
	return row.Count, err
}
