package deployments

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
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
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// spansByVersionRollupPrefix backs the migrated list-style deployment reads.
// Rollup schema is keyed (team_id, ts_bucket, service, service_version,
// environment) with first_seen/last_seen (min/max), request_count (sum),
// error_count (sum), latency_ms_digest (t-digest) plus any-state commit_sha,
// commit_author, repo_url, pr_url. span_count is derived from
// `count()`.
const (
	spansDeploysPrefix = "observability.spans"
)

// queryIntervalMinutes returns max(tierStep, dashboardStep).
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

// Git / VCS attribute expressions read from the raw spans JSON `attributes`
// column. Kept for the raw-scan drill-down paths (`GetImpactWindow`,
// `GetErrorGroupsWindow`, `GetEndpointMetricsWindow`) which still reach
// into the base `observability.spans` table.
const (
	attrCommitSHA    = "attributes.`git.commit.sha`::String"
	attrCommitAuthor = "attributes.`git.commit.author.name`::String"
	attrRepoURL      = "attributes.`scm.repository.url`::String"
	attrPRURL        = "attributes.`git.pull_request.url`::String"
)

const deploymentsJoinSelect = `
	WITH rollup_agg AS (
		SELECT service,
		       deployment_id,
		       min(timestamp)              AS first_seen,
		       max(timestamp)               AS last_seen,
		       toInt64(sum(span_count))     AS span_count
		FROM %s
		WHERE %s
		GROUP BY service, deployment_id
	),
	deployment_meta AS (
		SELECT team_id,
		       deployment_id,
		       argMax(service_version, last_seen) AS service_version,
		       argMax(environment, last_seen)     AS environment,
		       argMax(commit_sha, last_seen)      AS commit_sha,
		       argMax(commit_author, last_seen)   AS commit_author,
		       argMax(repo_url, last_seen)        AS repo_url,
		       argMax(pr_url, last_seen)          AS pr_url
		FROM observability.deployments
		WHERE team_id = @teamID
		GROUP BY team_id, deployment_id
	)
	SELECT r.service                        AS service,
	       d.service_version                     AS version,
	       d.environment                         AS environment,
	       r.first_seen                          AS first_seen,
	       r.last_seen                           AS last_seen,
	       r.span_count                          AS span_count,
	       d.commit_sha                          AS commit_sha,
	       d.commit_author                       AS commit_author,
	       d.repo_url                            AS repo_url,
	       d.pr_url                              AS pr_url
	FROM rollup_agg AS r
	LEFT JOIN deployment_meta AS d
	  ON d.deployment_id = r.deployment_id
	 AND d.team_id = @teamID`

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

// ListDeployments returns per-(version, environment) rollup of deploys for a
// single service, sourced from the spans_version cascade + deployments dim.
func (r *ClickHouseRepository) ListDeployments(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]deploymentAggRow, error) {
	table := "observability.spans"
	where := `team_id = @teamID AND service = @serviceName AND ts_bucket BETWEEN @start AND @end`
	query := fmt.Sprintf(deploymentsJoinSelect+" ORDER BY r.first_seen ASC", table, where)

	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.ListDeployments", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - tenant id fits uint32
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

// ListServiceDeployments returns every distinct deployment for a service
// across the compatibility view retention window.
func (r *ClickHouseRepository) ListServiceDeployments(ctx context.Context, teamID int64, serviceName string) ([]deploymentAggRow, error) {
	table := "observability." + spansDeploysPrefix
	where := `team_id = @teamID AND service = @serviceName`
	query := fmt.Sprintf(deploymentsJoinSelect+" ORDER BY r.first_seen ASC", table, where)

	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.ListServiceDeployments", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
	)
	return rows, err
}

// GetLatestDeploymentsByService returns the most recent (by first_seen)
// deployment per service for a team across the compatibility view retention window.
func (r *ClickHouseRepository) GetLatestDeploymentsByService(ctx context.Context, teamID int64) ([]deploymentAggRow, error) {
	table := "observability." + spansDeploysPrefix
	query := fmt.Sprintf(`
		WITH rollup_agg AS (
			SELECT service,
			       deployment_id,
			       min(timestamp)             AS first_seen,
			       max(timestamp)              AS last_seen,
			       toInt64(sum(span_count))    AS span_count
			FROM %s
			WHERE team_id = @teamID
			GROUP BY service, deployment_id
		),
		deployment_meta AS (
			SELECT team_id,
			       deployment_id,
			       argMax(service_version, last_seen) AS service_version,
			       argMax(environment, last_seen)     AS environment,
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
		SELECT r.service                        AS service,
		       d.service_version                     AS version,
		       d.environment                         AS environment,
		       r.first_seen                          AS first_seen,
		       r.last_seen                           AS last_seen,
		       r.span_count                          AS span_count,
		       d.commit_sha                          AS commit_sha,
		       d.commit_author                       AS commit_author,
		       d.repo_url                            AS repo_url,
		       d.pr_url                              AS pr_url
		FROM rollup_agg AS r
		INNER JOIN latest
		  ON r.service = latest.service
		 AND r.first_seen = latest.max_first_seen
		LEFT JOIN deployment_meta AS d
		  ON d.deployment_id = r.deployment_id
		 AND d.team_id = @teamID
		ORDER BY r.service ASC`, table)

	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetLatestDeploymentsByService", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
	)
	return rows, err
}

func (r *ClickHouseRepository) GetDeploysInRange(ctx context.Context, teamID int64, startMs, endMs int64) ([]deploymentAggRow, error) {
	table := "observability.spans"
	where := `team_id = @teamID AND ts_bucket BETWEEN @start AND @end`
	query := fmt.Sprintf(deploymentsJoinSelect+" ORDER BY r.first_seen ASC", table, where)

	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetDeploysInRange", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetVersionTraffic(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]VersionTrafficPoint, error) {
	bs := bucketSecs(startMs, endMs)
	table := "observability.spans"
	tierStep := int64(1)
	stepMin := queryIntervalMinutes(tierStep, startMs, endMs)
	query := fmt.Sprintf(`
		WITH deployment_meta AS (
			SELECT team_id,
			       deployment_id,
			       argMax(service_version, last_seen) AS service_version
			FROM observability.deployments
			WHERE team_id = @teamID
			GROUP BY team_id, deployment_id
		),
		agg AS (
			SELECT ts_bucket AS timestamp,
			       deployment_id,
			       count() / @bucketSeconds                      AS rps
			FROM %s
			WHERE team_id = @teamID
			  AND service = @serviceName
			  AND ts_bucket BETWEEN @start AND @end
			GROUP BY timestamp, deployment_id
		)
		SELECT agg.timestamp                 AS timestamp,
		       d.service_version             AS version,
		       agg.rps                       AS rps
		FROM agg
		LEFT JOIN deployment_meta AS d
		  ON d.deployment_id = agg.deployment_id
		 AND d.team_id = @teamID
		ORDER BY timestamp ASC, version ASC`, table)
	var rows []VersionTrafficPoint
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetVersionTraffic", &rows, query,
		clickhouse.Named("bucketSeconds", bs),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("intervalMin", stepMin),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetImpactWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (impactAggRow, error) {
	if endMs <= startMs {
		return impactAggRow{}, nil
	}
	table := "observability.spans"
	var row impactAggRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), fmt.Sprintf(`
		SELECT toInt64(count()) AS request_count,
		       toInt64(countIf(has_error OR toUInt16OrZero(response_status_code) >= 400))   AS error_count,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) AS p95_ms,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @start AND @end
	`, table),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	).ScanStruct(&row)
	if err != nil {
		return impactAggRow{}, err
	}
	return row, nil
}

// GetActiveVersion returns the most-recently-seen (version, environment) for
// a service inside the window. Computes deployment_id inline using the same
// cityHash64(service, version, environment) derivation that the
// spans_to_deployments MV uses (see db/clickhouse/05_dim_deployments.sql), so
// the join into observability.deployments resolves cleanly.
func (r *ClickHouseRepository) GetActiveVersion(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (activeVersionRow, error) {
	var rows []activeVersionRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetActiveVersion", &rows, `
		WITH agg AS (
			SELECT cityHash64(service, service_version, environment) AS deployment_id,
			       service_version,
			       environment AS environment,
			       max(timestamp) AS last_seen
			FROM observability.spans
			PREWHERE team_id = @teamID
			WHERE service = @serviceName
			  AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
			  AND timestamp BETWEEN @start AND @end
			  AND is_root = 1
			  AND service_version != ''
			GROUP BY service_version, environment
		)
		SELECT service_version AS version,
		       environment      AS environment
		FROM agg
		ORDER BY last_seen DESC
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
	table := "observability.spans"
	var rows []errorGroupAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetErrorGroupsWindow", &rows, fmt.Sprintf(`
		SELECT service                                AS service,
		       hex(status_message_hash)                    AS group_id,
		       operation_name                              AS operation_name,
		       any(status_message)             AS status_message,
		       toInt32(multiIf(http_status_bucket = '5xx', 500,
		                       http_status_bucket = '4xx', 400,
		                       0))                         AS http_status_code,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                       AS error_count,
		       max(timestamp)                         AS last_occurrence,
		       any(trace_id)                   AS sample_trace_id
		FROM %s
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY service, status_message_hash, operation_name, http_status_bucket
		ORDER BY error_count DESC
		LIMIT @limit
	`, table),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

// GetEndpointMetricsWindow reads per-endpoint RED from the spans_red rollup.
// spans_red keys by (service, operation, http_method, http_status_bucket);
// the previous `endpoint` dim was a duplicate of operation_name and is now
// surfaced as such in the DTO.
func (r *ClickHouseRepository) GetEndpointMetricsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]endpointMetricAggRow, error) {
	table := "observability.spans"
	var rows []endpointMetricAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetEndpointMetricsWindow", &rows, fmt.Sprintf(`
		SELECT operation_name                                                           AS operation_name,
		       operation_name                                                           AS endpoint_name,
		       http_method                                                              AS http_method,
		       toInt64(count())                                         AS request_count,
		       toInt64(countIf(has_error OR toUInt16OrZero(response_status_code) >= 400))                                           AS error_count,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])      AS p95_ms,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3])      AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY operation_name, http_method
		ORDER BY request_count DESC
		LIMIT @limit
	`, table),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}
