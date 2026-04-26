package deployments

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
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
// Rollup schema is keyed (team_id, bucket_ts, service_name, service_version,
// environment) with first_seen/last_seen (min/max), request_count (sum),
// error_count (sum), latency_ms_digest (t-digest) plus any-state commit_sha,
// commit_author, repo_url, pr_url. span_count is derived from
// `sumMerge(request_count)`.
const (
	spansDeploysPrefix         = rollup.FamilySpansDeploys
	spansByVersionPrefix       = rollup.FamilySpansVersion
	spansRollupPrefix          = rollup.FamilySpansRED
	errFingerprintRollupPrefix = rollup.FamilySpansErrors
)

// queryIntervalMinutes returns max(tierStep, dashboardStep). Copied from
// overview/overview/repository.go.
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

// commitMetaSelectRaw is the SELECT fragment for the four optional git/VCS
// attributes when reading from raw spans. Used by the drill-down queries
// that stayed on raw.
var commitMetaSelectRaw = fmt.Sprintf(
	"any(%s) AS commit_sha, any(%s) AS commit_author, any(%s) AS repo_url, any(%s) AS pr_url",
	attrCommitSHA, attrCommitAuthor, attrRepoURL, attrPRURL,
)

// deploymentsJoinSelect is the SELECT + FROM fragment used by every
// rollup-backed list query. It aggregates the rollup by deployment_id, then
// LEFT JOINs observability.deployments (the dim table) to pull VCS metadata
// (service_version, environment, commit_sha, commit_author, repo_url, pr_url).
// Callers supply `{table}` (the tier-specific rollup table), a WHERE clause
// for the CTE, and ORDER BY.
//
// The dim table is ReplacingMergeTree(last_seen) keyed on
// (team_id, service_name, service_version, environment); FINAL collapses
// duplicates on read.
const deploymentsJoinSelect = `
	WITH rollup_agg AS (
		SELECT service_name,
		       deployment_id,
		       minMerge(first_seen)              AS first_seen,
		       maxMerge(last_seen)               AS last_seen,
		       toInt64(sumMerge(span_count))     AS span_count
		FROM %s
		WHERE %s
		GROUP BY service_name, deployment_id
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
	SELECT r.service_name                        AS service_name,
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
	table := rollup.For(spansDeploysPrefix, startMs, endMs).Table
	where := `team_id = @teamID AND service_name = @serviceName AND bucket_ts BETWEEN @start AND @end`
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
// across the entire rollup retention window. Pins to the _1h tier (90-day
// retention) rather than calling rollup.For since the caller does not
// bound the window.
func (r *ClickHouseRepository) ListServiceDeployments(ctx context.Context, teamID int64, serviceName string) ([]deploymentAggRow, error) {
	table := "observability." + spansDeploysPrefix + "_1h"
	where := `team_id = @teamID AND service_name = @serviceName`
	query := fmt.Sprintf(deploymentsJoinSelect+" ORDER BY r.first_seen ASC", table, where)

	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.ListServiceDeployments", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
	)
	return rows, err
}

// GetLatestDeploymentsByService returns the most recent (by first_seen)
// deployment per service for a team, across the 90-day _1h tier.
func (r *ClickHouseRepository) GetLatestDeploymentsByService(ctx context.Context, teamID int64) ([]deploymentAggRow, error) {
	table := "observability." + spansDeploysPrefix + "_1h"
	query := fmt.Sprintf(`
		WITH rollup_agg AS (
			SELECT service_name,
			       deployment_id,
			       minMerge(first_seen)             AS first_seen,
			       maxMerge(last_seen)              AS last_seen,
			       toInt64(sumMerge(span_count))    AS span_count
			FROM %s
			WHERE team_id = @teamID
			GROUP BY service_name, deployment_id
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
			SELECT service_name, max(first_seen) AS max_first_seen
			FROM rollup_agg
			GROUP BY service_name
		)
		SELECT r.service_name                        AS service_name,
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
		  ON r.service_name = latest.service_name
		 AND r.first_seen = latest.max_first_seen
		LEFT JOIN deployment_meta AS d
		  ON d.deployment_id = r.deployment_id
		 AND d.team_id = @teamID
		ORDER BY r.service_name ASC`, table)

	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetLatestDeploymentsByService", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
	)
	return rows, err
}

// GetDeploysInRange returns all deploys in [startMs,endMs] across every
// service for a team. Used by alerting for fire/resolve deploy correlation.
func (r *ClickHouseRepository) GetDeploysInRange(ctx context.Context, teamID int64, startMs, endMs int64) ([]deploymentAggRow, error) {
	table := rollup.For(spansDeploysPrefix, startMs, endMs).Table
	where := `team_id = @teamID AND bucket_ts BETWEEN @start AND @end`
	query := fmt.Sprintf(deploymentsJoinSelect+" ORDER BY r.first_seen ASC", table, where)

	var rows []deploymentAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetDeploysInRange", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

// GetVersionTraffic returns per-version request rate time series. Groups the
// rollup by (bucket, deployment_id), then joins the dim table to expose
// service_version as the DTO's "version" field.
func (r *ClickHouseRepository) GetVersionTraffic(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]VersionTrafficPoint, error) {
	bs := bucketSecs(startMs, endMs)
	tier := rollup.For(spansByVersionPrefix, startMs, endMs)
	table, tierStep := tier.Table, tier.StepMin
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
			SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS timestamp,
			       deployment_id,
			       sumMerge(request_count) / @bucketSeconds                      AS rps
			FROM %s
			WHERE team_id = @teamID
			  AND service_name = @serviceName
			  AND bucket_ts BETWEEN @start AND @end
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

// --- Drill-window queries: rollup-backed (Phase 9) ---
//
// Every drill-window method below fits an existing rollup. Each composes its
// answer from one of:
//   - spans_rollup              — RED per (service, operation, endpoint, method)
//   - spans_by_version          — last_seen / version-environment association
//   - spans_error_fingerprint   — grouped error spans with sample trace_id
//
// No raw span reads remain in this file. Tracedetail remains raw
// (per-trace drill-down, bounded by idx_trace_id).

func (r *ClickHouseRepository) GetImpactWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (impactAggRow, error) {
	if endMs <= startMs {
		return impactAggRow{}, nil
	}
	table := rollup.For(spansRollupPrefix, startMs, endMs).Table
	var row impactAggRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), fmt.Sprintf(`
		SELECT toInt64(sumMerge(request_count)) AS request_count,
		       toInt64(sumMerge(error_count))   AS error_count,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) AS p95_ms,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND service_name = @serviceName
		  AND bucket_ts BETWEEN @start AND @end
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
// a service inside the window. Uses the 1m rollup (exact max, not summary),
// joined with the deployments dim for the version string.
func (r *ClickHouseRepository) GetActiveVersion(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (activeVersionRow, error) {
	var rows []activeVersionRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetActiveVersion", &rows, `
		WITH deployment_meta AS (
			SELECT team_id,
			       deployment_id,
			       argMax(service_version, last_seen) AS service_version,
			       argMax(environment, last_seen)     AS environment
			FROM observability.deployments
			WHERE team_id = @teamID
			GROUP BY team_id, deployment_id
		),
		agg AS (
			SELECT deployment_id,
			       maxMerge(last_seen) AS last_seen
			FROM observability.spans_deploys_1m
			WHERE team_id = @teamID
			  AND service_name = @serviceName
			  AND bucket_ts BETWEEN @start AND @end
			GROUP BY deployment_id
		)
		SELECT d.service_version AS version,
		       d.environment     AS environment
		FROM agg
		LEFT JOIN deployment_meta AS d
		  ON d.deployment_id = agg.deployment_id
		 AND d.team_id = @teamID
		ORDER BY agg.last_seen DESC
		LIMIT 1
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	if err != nil || len(rows) == 0 {
		return activeVersionRow{}, err
	}
	return rows[0], nil
}

// GetErrorGroupsWindow returns top error groups inside a deploy window via
// `spans_error_fingerprint` (same rollup overview/errors uses). Rollup keys
// include service_name + operation_name + exception_type + status_message_hash
// + http_status_bucket; state carries sample_trace_id + last_seen. `group_id`
// is the fingerprint hash, rendered as a hex string for client use.
func (r *ClickHouseRepository) GetErrorGroupsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]errorGroupAggRow, error) {
	table := rollup.For(errFingerprintRollupPrefix, startMs, endMs).Table
	var rows []errorGroupAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetErrorGroupsWindow", &rows, fmt.Sprintf(`
		SELECT service_name                                AS service_name,
		       hex(status_message_hash)                    AS group_id,
		       operation_name                              AS operation_name,
		       anyMerge(sample_status_message)             AS status_message,
		       toInt32(multiIf(http_status_bucket = '5xx', 500,
		                       http_status_bucket = '4xx', 400,
		                       0))                         AS http_status_code,
		       sumMerge(error_count)                       AS error_count,
		       maxMerge(last_seen)                         AS last_occurrence,
		       anyMerge(sample_trace_id)                   AS sample_trace_id
		FROM %s
		WHERE team_id = @teamID
		  AND service_name = @serviceName
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY service_name, status_message_hash, operation_name, http_status_bucket
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
	table := rollup.For(spansRollupPrefix, startMs, endMs).Table
	var rows []endpointMetricAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "deployments.GetEndpointMetricsWindow", &rows, fmt.Sprintf(`
		SELECT operation_name                                                           AS operation_name,
		       operation_name                                                           AS endpoint_name,
		       http_method                                                              AS http_method,
		       toInt64(sumMerge(request_count))                                         AS request_count,
		       toInt64(sumMerge(error_count))                                           AS error_count,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])      AS p95_ms,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3])      AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND service_name = @serviceName
		  AND bucket_ts BETWEEN @start AND @end
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
