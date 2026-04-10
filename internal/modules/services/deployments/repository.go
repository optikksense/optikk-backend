package deployments

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
)

// Repository runs ClickHouse queries for deployment detection.
type Repository interface {
	ListDeployments(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]deploymentAggRow, error)
	ListServiceDeployments(ctx context.Context, teamID int64, serviceName string) ([]deploymentAggRow, error)
	GetLatestDeploymentsByService(ctx context.Context, teamID int64) ([]deploymentAggRow, error)
	GetDeploysInRange(ctx context.Context, teamID int64, startMs, endMs int64) ([]deploymentAggRow, error)
	GetVersionTraffic(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]VersionTrafficPoint, error)
	GetImpactWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (impactAggRow, error)
	GetActiveVersion(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (activeVersionRow, error)
	GetErrorGroupsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]errorGroupAggRow, error)
	GetEndpointMetricsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]endpointMetricAggRow, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func bucketSecs(startMs, endMs int64) float64 {
	h := (endMs - startMs) / 3_600_000
	switch {
	case h <= 3:
		return 60.0
	case h <= 12:
		return 300.0
	default:
		return 3600.0
	}
}

func (r *ClickHouseRepository) ListDeployments(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]deploymentAggRow, error) {
	var rows []deploymentAggRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.service_name AS service_name,
		       mat_service_version AS version,
		       mat_deployment_environment AS environment,
		       min(s.timestamp) AS first_seen,
		       max(s.timestamp) AS last_seen,
		       toInt64(count()) AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.service_name = @serviceName
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+rootspan.Condition("s")+`
		  AND s.mat_service_version != ''
		GROUP BY version, environment
		ORDER BY first_seen ASC
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) ListServiceDeployments(ctx context.Context, teamID int64, serviceName string) ([]deploymentAggRow, error) {
	var rows []deploymentAggRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.service_name AS service_name,
		       s.mat_service_version AS version,
		       s.mat_deployment_environment AS environment,
		       min(s.timestamp) AS first_seen,
		       max(s.timestamp) AS last_seen,
		       toInt64(count()) AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.service_name = @serviceName
		  AND `+rootspan.Condition("s")+`
		  AND s.mat_service_version != ''
		GROUP BY service_name, version, environment
		ORDER BY first_seen ASC
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetLatestDeploymentsByService(ctx context.Context, teamID int64) ([]deploymentAggRow, error) {
	var rows []deploymentAggRow
	err := r.db.Select(ctx, &rows, `
		WITH deployments AS (
			SELECT s.service_name AS service_name,
			       s.mat_service_version AS version,
			       s.mat_deployment_environment AS environment,
			       min(s.timestamp) AS first_seen,
			       max(s.timestamp) AS last_seen,
			       toInt64(count()) AS span_count
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND `+rootspan.Condition("s")+`
			  AND s.mat_service_version != ''
			GROUP BY service_name, version, environment
		),
		latest AS (
			SELECT service_name, max(first_seen) AS max_first_seen
			FROM deployments
			GROUP BY service_name
		)
		SELECT deployments.service_name AS service_name,
		       deployments.version AS version,
		       deployments.environment AS environment,
		       deployments.first_seen AS first_seen,
		       deployments.last_seen AS last_seen,
		       deployments.span_count AS span_count
		FROM deployments
		INNER JOIN latest
		  ON deployments.service_name = latest.service_name
		 AND deployments.first_seen = latest.max_first_seen
		ORDER BY deployments.service_name ASC
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
	)
	return rows, err
}

// GetDeploysInRange returns all deploys (root-span markers) in [startMs,endMs]
// across every service for a team. Used by alerting for fire/resolve deploy
// correlation.
func (r *ClickHouseRepository) GetDeploysInRange(ctx context.Context, teamID int64, startMs, endMs int64) ([]deploymentAggRow, error) {
	var rows []deploymentAggRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.service_name AS service_name,
		       s.mat_service_version AS version,
		       s.mat_deployment_environment AS environment,
		       min(s.timestamp) AS first_seen,
		       max(s.timestamp) AS last_seen,
		       toInt64(count()) AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+rootspan.Condition("s")+`
		  AND s.mat_service_version != ''
		GROUP BY service_name, version, environment
		ORDER BY first_seen ASC
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetVersionTraffic(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]VersionTrafficPoint, error) {
	bs := bucketSecs(startMs, endMs)
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.mat_service_version AS version,
		       count() / @bucketSeconds AS rps
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.service_name = @serviceName
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+rootspan.Condition("s")+`
		  AND s.mat_service_version != ''
		GROUP BY timestamp, version
		ORDER BY timestamp ASC, version ASC
	`, bucket)
	var rows []VersionTrafficPoint
	err := r.db.Select(ctx, &rows, query,
		clickhouse.Named("bucketSeconds", bs),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetImpactWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (impactAggRow, error) {
	if endMs <= startMs {
		return impactAggRow{}, nil
	}
	var row impactAggRow
	err := r.db.QueryRow(ctx, &row, `
		SELECT toInt64(count()) AS request_count,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) AS error_count,
		       quantileExact(0.95)(s.duration_nano / 1000000.0) AS p95_ms,
		       quantileExact(0.99)(s.duration_nano / 1000000.0) AS p99_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.service_name = @serviceName
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp >= @start AND s.timestamp < @end
		  AND `+rootspan.Condition("s")+`
		  AND s.mat_service_version != ''
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	if err != nil {
		return impactAggRow{}, err
	}
	return row, nil
}

func (r *ClickHouseRepository) GetActiveVersion(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (activeVersionRow, error) {
	var rows []activeVersionRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.mat_service_version AS version,
		       s.mat_deployment_environment AS environment
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.service_name = @serviceName
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+rootspan.Condition("s")+`
		  AND s.mat_service_version != ''
		ORDER BY s.timestamp DESC
		LIMIT 1
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	if err != nil || len(rows) == 0 {
		return activeVersionRow{}, err
	}
	return rows[0], nil
}

func (r *ClickHouseRepository) GetErrorGroupsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]errorGroupAggRow, error) {
	var rows []errorGroupAggRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.service_name AS service_name,
		       '' AS group_id,
		       s.name AS operation_name,
		       s.status_message AS status_message,
		       toUInt16OrZero(s.response_status_code) AS http_status_code,
		       count() AS error_count,
		       max(s.timestamp) AS last_occurrence,
		       argMax(s.trace_id, s.timestamp) AS sample_trace_id
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.service_name = @serviceName
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp >= @start AND s.timestamp < @end
		  AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)
		GROUP BY service_name, operation_name, status_message, http_status_code
		ORDER BY error_count DESC
		LIMIT @limit
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetEndpointMetricsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]endpointMetricAggRow, error) {
	var rows []endpointMetricAggRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.name AS operation_name,
		       coalesce(nullIf(s.mat_http_route, ''), nullIf(s.mat_http_target, ''), s.name) AS endpoint_name,
		       s.http_method AS http_method,
		       toInt64(count()) AS request_count,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) AS error_count,
		       quantile(`+fmt.Sprintf("%.2f", 0.95)+`)(s.duration_nano / 1000000.0) AS p95_ms,
		       quantile(`+fmt.Sprintf("%.2f", 0.99)+`)(s.duration_nano / 1000000.0) AS p99_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.service_name = @serviceName
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp >= @start AND s.timestamp < @end
		  AND `+rootspan.Condition("s")+`
		GROUP BY operation_name, endpoint_name, http_method
		ORDER BY request_count DESC
		LIMIT @limit
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}
