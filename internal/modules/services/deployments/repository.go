package deployments

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
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
const spansByVersionPrefix = "observability.spans_by_version"

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

// commitMetaSelectRollup is the equivalent for rollup reads — the any-state
// aggregates already live in the rollup and only need an `-Merge` combinator.
const commitMetaSelectRollup = `anyMerge(commit_sha) AS commit_sha,
anyMerge(commit_author) AS commit_author,
anyMerge(repo_url) AS repo_url,
anyMerge(pr_url) AS pr_url`

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
// single service, sourced from `spans_by_version_*` cascade.
func (r *ClickHouseRepository) ListDeployments(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]deploymentAggRow, error) {
	table, _ := rollup.TierTableFor(spansByVersionPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name                                 AS service_name,
		       service_version                              AS version,
		       environment                                  AS environment,
		       minMerge(first_seen)                         AS first_seen,
		       maxMerge(last_seen)                          AS last_seen,
		       toInt64(sumMerge(request_count))             AS span_count,
		       %s
		FROM %s
		WHERE team_id = @teamID
		  AND service_name = @serviceName
		  AND bucket_ts BETWEEN @start AND @end
		  AND service_version != ''
		GROUP BY service_name, version, environment
		ORDER BY first_seen ASC`, commitMetaSelectRollup, table)

	var rows []deploymentAggRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - tenant id fits uint32
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

// ListServiceDeployments returns every distinct (version, environment)
// deployment for a service across the entire rollup retention window.
func (r *ClickHouseRepository) ListServiceDeployments(ctx context.Context, teamID int64, serviceName string) ([]deploymentAggRow, error) {
	// `spans_by_version_1h` gives us the whole 90-day retention in the
	// cheapest form; explicitly pin to it rather than calling TierTableFor
	// since the caller does not bound the window.
	table := spansByVersionPrefix + "_1h"
	query := fmt.Sprintf(`
		SELECT service_name                                 AS service_name,
		       service_version                              AS version,
		       environment                                  AS environment,
		       minMerge(first_seen)                         AS first_seen,
		       maxMerge(last_seen)                          AS last_seen,
		       toInt64(sumMerge(request_count))             AS span_count,
		       %s
		FROM %s
		WHERE team_id = @teamID
		  AND service_name = @serviceName
		  AND service_version != ''
		GROUP BY service_name, version, environment
		ORDER BY first_seen ASC`, commitMetaSelectRollup, table)

	var rows []deploymentAggRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
	)
	return rows, err
}

// GetLatestDeploymentsByService returns the most recent (by first_seen)
// deployment per service across all teams' services.
func (r *ClickHouseRepository) GetLatestDeploymentsByService(ctx context.Context, teamID int64) ([]deploymentAggRow, error) {
	table := spansByVersionPrefix + "_1h"
	query := fmt.Sprintf(`
		WITH deployments AS (
			SELECT service_name                         AS service_name,
			       service_version                      AS version,
			       environment                          AS environment,
			       minMerge(first_seen)                 AS first_seen,
			       maxMerge(last_seen)                  AS last_seen,
			       toInt64(sumMerge(request_count))     AS span_count,
			       %s
			FROM %s
			WHERE team_id = @teamID
			  AND service_version != ''
			GROUP BY service_name, version, environment
		),
		latest AS (
			SELECT service_name, max(first_seen) AS max_first_seen
			FROM deployments
			GROUP BY service_name
		)
		SELECT deployments.service_name  AS service_name,
		       deployments.version        AS version,
		       deployments.environment    AS environment,
		       deployments.first_seen     AS first_seen,
		       deployments.last_seen      AS last_seen,
		       deployments.span_count     AS span_count,
		       deployments.commit_sha     AS commit_sha,
		       deployments.commit_author  AS commit_author,
		       deployments.repo_url       AS repo_url,
		       deployments.pr_url         AS pr_url
		FROM deployments
		INNER JOIN latest
		  ON deployments.service_name = latest.service_name
		 AND deployments.first_seen = latest.max_first_seen
		ORDER BY deployments.service_name ASC`, commitMetaSelectRollup, table)

	var rows []deploymentAggRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
	)
	return rows, err
}

// GetDeploysInRange returns all deploys in [startMs,endMs] across every
// service for a team. Used by alerting for fire/resolve deploy correlation.
func (r *ClickHouseRepository) GetDeploysInRange(ctx context.Context, teamID int64, startMs, endMs int64) ([]deploymentAggRow, error) {
	table, _ := rollup.TierTableFor(spansByVersionPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name                                 AS service_name,
		       service_version                              AS version,
		       environment                                  AS environment,
		       minMerge(first_seen)                         AS first_seen,
		       maxMerge(last_seen)                          AS last_seen,
		       toInt64(sumMerge(request_count))             AS span_count,
		       %s
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND service_version != ''
		GROUP BY service_name, version, environment
		ORDER BY first_seen ASC`, commitMetaSelectRollup, table)

	var rows []deploymentAggRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

// GetVersionTraffic returns per-version request rate time series. Sourced
// from `spans_by_version_*` — the rollup's `request_count` merged state
// gives us the per-bucket count; we divide by the bucket width to derive
// RPS.
func (r *ClickHouseRepository) GetVersionTraffic(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]VersionTrafficPoint, error) {
	bs := bucketSecs(startMs, endMs)
	table, tierStep := rollup.TierTableFor(spansByVersionPrefix, startMs, endMs)
	stepMin := queryIntervalMinutes(tierStep, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS timestamp,
		       service_version                                               AS version,
		       sumMerge(request_count) / @bucketSeconds                      AS rps
		FROM %s
		WHERE team_id = @teamID
		  AND service_name = @serviceName
		  AND bucket_ts BETWEEN @start AND @end
		  AND service_version != ''
		GROUP BY timestamp, version
		ORDER BY timestamp ASC, version ASC`, table)
	var rows []VersionTrafficPoint
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query,
		clickhouse.Named("bucketSeconds", bs),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("intervalMin", stepMin),
	)
	return rows, err
}

// --- Raw-scan drill-down queries (unchanged) ---

func (r *ClickHouseRepository) GetImpactWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (impactAggRow, error) {
	if endMs <= startMs {
		return impactAggRow{}, nil
	}
	var row impactAggRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), `
		SELECT toInt64(count()) AS request_count,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) AS error_count,
		       quantileTDigest(0.95)(s.duration_nano / 1000000.0) AS p95_ms,
		       quantileTDigest(0.99)(s.duration_nano / 1000000.0) AS p99_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.service_name = @serviceName
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp >= @start AND s.timestamp < @end
		  AND `+rootspan.Condition("s")+`
		  AND s.mat_service_version != ''
	`, clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName), clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)), clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)), clickhouse.Named("start", time.UnixMilli(startMs)), clickhouse.Named("end", time.UnixMilli(endMs)), ).ScanStruct(&row)
	if err != nil {
		return impactAggRow{}, err
	}
	return row, nil
}

func (r *ClickHouseRepository) GetActiveVersion(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (activeVersionRow, error) {
	var rows []activeVersionRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
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
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
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
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT s.name AS operation_name,
		       coalesce(nullIf(s.mat_http_route, ''), nullIf(s.mat_http_target, ''), s.name) AS endpoint_name,
		       s.http_method AS http_method,
		       toInt64(count()) AS request_count,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) AS error_count,
		       quantileTDigest(`+fmt.Sprintf("%.2f", 0.95)+`)(s.duration_nano / 1000000.0) AS p95_ms,
		       quantileTDigest(`+fmt.Sprintf("%.2f", 0.99)+`)(s.duration_nano / 1000000.0) AS p99_ms
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
