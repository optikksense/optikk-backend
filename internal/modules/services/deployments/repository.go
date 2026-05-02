package deployments

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// Repository runs ClickHouse queries for deployment detection. Reads
// observability.spans_1m for span aggregates (request/error counts, latency
// percentiles via quantileTimingMerge) and joins observability.deployments for
// VCS metadata (commit_sha, commit_author, repo_url, pr_url). deployment_id is
// computed inline via cityHash64(service, service_version, environment) — same
// derivation used by the spans_to_deployments MV in 05_deployments.sql.
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
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
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

const deploymentsJoinSelect = `
	WITH rollup_agg AS (
		SELECT service                                                            AS service,
		       service_version                                                    AS service_version,
		       environment                                                        AS environment,
		       cityHash64(service, service_version, environment)                  AS deployment_id,
		       min(timestamp)                                                     AS first_seen,
		       max(timestamp)                                                     AS last_seen,
		       toInt64(sum(request_count))                                        AS span_count
		FROM observability.spans_1m
		WHERE %s
		  AND service_version != ''
		GROUP BY service, service_version, environment
	),
	deployment_meta AS (
		SELECT team_id,
		       deployment_id,
		       argMax(commit_sha, last_seen)      AS commit_sha,
		       argMax(commit_author, last_seen)   AS commit_author,
		       argMax(repo_url, last_seen)        AS repo_url,
		       argMax(pr_url, last_seen)          AS pr_url
		FROM observability.deployments
		WHERE team_id = @teamID
		GROUP BY team_id, deployment_id
	)
	SELECT r.service                          AS service,
	       r.service_version                  AS version,
	       r.environment                      AS environment,
	       r.first_seen                       AS first_seen,
	       r.last_seen                        AS last_seen,
	       r.span_count                       AS span_count,
	       d.commit_sha                       AS commit_sha,
	       d.commit_author                    AS commit_author,
	       d.repo_url                         AS repo_url,
	       d.pr_url                           AS pr_url
	FROM rollup_agg AS r
	LEFT JOIN deployment_meta AS d
	  ON d.deployment_id = r.deployment_id
	 AND d.team_id = @teamID`

func (r *ClickHouseRepository) ListDeployments(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]deploymentAggRow, error) {
	const where = `team_id = @teamID AND service = @serviceName AND ts_bucket BETWEEN @bucketStart AND @bucketEnd`
	query := deploymentsJoinSelectWith(where) + " ORDER BY r.first_seen ASC"
	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.ListDeployments", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
	)
	return rows, err
}

func (r *ClickHouseRepository) ListServiceDeployments(ctx context.Context, teamID int64, serviceName string) ([]deploymentAggRow, error) {
	const where = `team_id = @teamID AND service = @serviceName`
	query := deploymentsJoinSelectWith(where) + " ORDER BY r.first_seen ASC"
	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.ListServiceDeployments", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetLatestDeploymentsByService(ctx context.Context, teamID int64) ([]deploymentAggRow, error) {
	const query = `
		WITH rollup_agg AS (
			SELECT service                                                            AS service,
			       service_version                                                    AS service_version,
			       environment                                                        AS environment,
			       cityHash64(service, service_version, environment)                  AS deployment_id,
			       min(timestamp)                                                     AS first_seen,
			       max(timestamp)                                                     AS last_seen,
			       toInt64(sum(request_count))                                        AS span_count
			FROM observability.spans_1m
			WHERE team_id = @teamID
			  AND service_version != ''
			GROUP BY service, service_version, environment
		),
		deployment_meta AS (
			SELECT team_id,
			       deployment_id,
			       argMax(commit_sha, last_seen)      AS commit_sha,
			       argMax(commit_author, last_seen)   AS commit_author,
			       argMax(repo_url, last_seen)        AS repo_url,
			       argMax(pr_url, last_seen)          AS pr_url
			FROM observability.deployments
			WHERE team_id = @teamID
			GROUP BY team_id, deployment_id
		),
		latest AS (
			SELECT service, max(first_seen) AS max_first_seen
			FROM rollup_agg
			GROUP BY service
		)
		SELECT r.service                          AS service,
		       r.service_version                  AS version,
		       r.environment                      AS environment,
		       r.first_seen                       AS first_seen,
		       r.last_seen                        AS last_seen,
		       r.span_count                       AS span_count,
		       d.commit_sha                       AS commit_sha,
		       d.commit_author                    AS commit_author,
		       d.repo_url                         AS repo_url,
		       d.pr_url                           AS pr_url
		FROM rollup_agg AS r
		INNER JOIN latest
		  ON r.service = latest.service
		 AND r.first_seen = latest.max_first_seen
		LEFT JOIN deployment_meta AS d
		  ON d.deployment_id = r.deployment_id
		 AND d.team_id = @teamID
		ORDER BY r.service ASC`
	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetLatestDeploymentsByService", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
	)
	return rows, err
}

func (r *ClickHouseRepository) GetDeploysInRange(ctx context.Context, teamID int64, startMs, endMs int64) ([]deploymentAggRow, error) {
	const where = `team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd`
	query := deploymentsJoinSelectWith(where) + " ORDER BY r.first_seen ASC"
	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetDeploysInRange", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetVersionTraffic(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]VersionTrafficPoint, error) {
	const query = `
		WITH agg AS (
			SELECT toDateTime(ts_bucket)                              AS timestamp,
			       service_version                                    AS service_version,
			       sum(request_count) / @bucketSeconds                AS rps
			FROM observability.spans_1m
			WHERE team_id = @teamID
			  AND service = @serviceName
			  AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
			  AND service_version != ''
			GROUP BY timestamp, service_version
		)
		SELECT timestamp                  AS timestamp,
		       service_version            AS version,
		       rps                        AS rps
		FROM agg
		ORDER BY timestamp ASC, version ASC`
	var rows []VersionTrafficPoint
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetVersionTraffic", &rows, query,
		clickhouse.Named("bucketSeconds", bucketSecs(startMs, endMs)),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetImpactWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (impactAggRow, error) {
	if endMs <= startMs {
		return impactAggRow{}, nil
	}
	const query = `
		SELECT toInt64(sum(request_count))                                AS request_count,
		       toInt64(sum(error_count))                                  AS error_count,
		       toFloat64(quantileTimingMerge(0.95)(latency_state))        AS p95_ms,
		       toFloat64(quantileTimingMerge(0.99)(latency_state))        AS p99_ms
		FROM observability.spans_1m
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @bucketStart AND @bucketEnd`
	var row impactAggRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
	).ScanStruct(&row)
	if err != nil {
		return impactAggRow{}, err
	}
	return row, nil
}

func (r *ClickHouseRepository) GetActiveVersion(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (activeVersionRow, error) {
	const query = `
		WITH agg AS (
			SELECT service_version                AS service_version,
			       environment                    AS environment,
			       max(timestamp)                 AS last_seen
			FROM observability.spans_1m
			WHERE team_id = @teamID
			  AND service = @serviceName
			  AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
			  AND is_root = 1
			  AND service_version != ''
			GROUP BY service_version, environment
		)
		SELECT service_version AS version,
		       environment     AS environment
		FROM agg
		ORDER BY last_seen DESC
		LIMIT 1`
	var rows []activeVersionRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetActiveVersion", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
	)
	if err != nil || len(rows) == 0 {
		return activeVersionRow{}, err
	}
	return rows[0], nil
}

func (r *ClickHouseRepository) GetErrorGroupsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]errorGroupAggRow, error) {
	const query = `
		SELECT service                                                    AS service,
		       hex(status_message_hash)                                   AS group_id,
		       name                                                       AS operation_name,
		       any(sample_status_message)                                 AS status_message,
		       toInt32(multiIf(http_status_bucket = '5xx', 500,
		                       http_status_bucket = '4xx', 400,
		                       0))                                        AS http_status_code,
		       sum(error_count)                                           AS error_count,
		       max(timestamp)                                             AS last_occurrence,
		       any(sample_trace_id)                                       AS sample_trace_id
		FROM observability.spans_1m
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		GROUP BY service, status_message_hash, name, http_status_bucket
		HAVING error_count > 0
		ORDER BY error_count DESC
		LIMIT @limit`
	var rows []errorGroupAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetErrorGroupsWindow", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetEndpointMetricsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]endpointMetricAggRow, error) {
	const query = `
		SELECT name                                                            AS operation_name,
		       name                                                            AS endpoint_name,
		       http_method                                                     AS http_method,
		       toInt64(sum(request_count))                                     AS request_count,
		       toInt64(sum(error_count))                                       AS error_count,
		       toFloat64(quantileTimingMerge(0.95)(latency_state))             AS p95_ms,
		       toFloat64(quantileTimingMerge(0.99)(latency_state))             AS p99_ms
		FROM observability.spans_1m
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		GROUP BY name, http_method
		ORDER BY request_count DESC
		LIMIT @limit`
	var rows []endpointMetricAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetEndpointMetricsWindow", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

// deploymentsJoinSelectWith inlines the per-method WHERE predicate into the
// shared rollup_agg + deployment_meta JOIN template. We can't fmt.Sprintf SQL
// values per project rules, but the where clause itself is a const fragment
// not derived from any user input; method-specific predicates are bound via
// clickhouse.Named() args. Each caller passes a fixed const string here.
func deploymentsJoinSelectWith(whereClause string) string {
	return `
	WITH rollup_agg AS (
		SELECT service                                                            AS service,
		       service_version                                                    AS service_version,
		       environment                                                        AS environment,
		       cityHash64(service, service_version, environment)                  AS deployment_id,
		       min(timestamp)                                                     AS first_seen,
		       max(timestamp)                                                     AS last_seen,
		       toInt64(sum(request_count))                                        AS span_count
		FROM observability.spans_1m
		WHERE ` + whereClause + `
		  AND service_version != ''
		GROUP BY service, service_version, environment
	),
	deployment_meta AS (
		SELECT team_id,
		       deployment_id,
		       argMax(commit_sha, last_seen)      AS commit_sha,
		       argMax(commit_author, last_seen)   AS commit_author,
		       argMax(repo_url, last_seen)        AS repo_url,
		       argMax(pr_url, last_seen)          AS pr_url
		FROM observability.deployments
		WHERE team_id = @teamID
		GROUP BY team_id, deployment_id
	)
	SELECT r.service                          AS service,
	       r.service_version                  AS version,
	       r.environment                      AS environment,
	       r.first_seen                       AS first_seen,
	       r.last_seen                        AS last_seen,
	       r.span_count                       AS span_count,
	       d.commit_sha                       AS commit_sha,
	       d.commit_author                    AS commit_author,
	       d.repo_url                         AS repo_url,
	       d.pr_url                           AS pr_url
	FROM rollup_agg AS r
	LEFT JOIN deployment_meta AS d
	  ON d.deployment_id = r.deployment_id
	 AND d.team_id = @teamID`
}

// queryIntervalMinutes returns the dashboard-step in minutes for the window.
// Kept exported-internal for service-layer logic that bins by this step.
func queryIntervalMinutes(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var dashStep int64
	switch {
	case hours <= 3:
		dashStep = 1
	case hours <= 24:
		dashStep = 5
	case hours <= 168:
		dashStep = 60
	default:
		dashStep = 1440
	}
	if tierStepMin > dashStep {
		return tierStepMin
	}
	return dashStep
}

