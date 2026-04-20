package deployments

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
	"golang.org/x/sync/errgroup"
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

// Git / VCS attribute expressions read from the spans JSON `attributes`
// column. Backtick-quoted keys are required because the paths contain
// dots; kept as constants so the backticks don't collide with the
// raw-string SQL in the queries below.
const (
	attrCommitSHA    = "attributes.`git.commit.sha`::String"
	attrCommitAuthor = "attributes.`git.commit.author.name`::String"
	attrRepoURL      = "attributes.`scm.repository.url`::String"
	attrPRURL        = "attributes.`git.pull_request.url`::String"
)

// commitMetaSelect is the reusable SELECT fragment for the four optional
// git/VCS attributes on a deployment aggregation. `any(...)` is safe
// because these values are constant within a (version, environment) pair
// when the emitting instrumentation tags it correctly.
var commitMetaSelect = fmt.Sprintf(
	"any(%s) AS commit_sha, any(%s) AS commit_author, any(%s) AS repo_url, any(%s) AS pr_url",
	attrCommitSHA, attrCommitAuthor, attrRepoURL, attrPRURL,
)

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

// deploymentAggScanRow is the repository-side scan target. CH's count() is
// UInt64; the external `deploymentAggRow` keeps SpanCount as int64 for the
// public model, so we scan into uint64 here and convert at the boundary.
type deploymentAggScanRow struct {
	ServiceName  string    `ch:"service_name"`
	Version      string    `ch:"version"`
	Environment  string    `ch:"environment"`
	FirstSeen    time.Time `ch:"first_seen"`
	LastSeen     time.Time `ch:"last_seen"`
	SpanCount    uint64    `ch:"span_count"`
	CommitSHA    string    `ch:"commit_sha"`
	CommitAuthor string    `ch:"commit_author"`
	RepoURL      string    `ch:"repo_url"`
	PRURL        string    `ch:"pr_url"`
}

func toDeploymentAggRows(in []deploymentAggScanRow) []deploymentAggRow {
	out := make([]deploymentAggRow, len(in))
	for i, r := range in {
		out[i] = deploymentAggRow{
			ServiceName:  r.ServiceName,
			Version:      r.Version,
			Environment:  r.Environment,
			FirstSeen:    r.FirstSeen,
			LastSeen:     r.LastSeen,
			SpanCount:    int64(r.SpanCount), //nolint:gosec // bounded by query window
			CommitSHA:    r.CommitSHA,
			CommitAuthor: r.CommitAuthor,
			RepoURL:      r.RepoURL,
			PRURL:        r.PRURL,
		}
	}
	return out
}

func (r *ClickHouseRepository) ListDeployments(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]deploymentAggRow, error) {
	var rows []deploymentAggScanRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT s.service_name AS service_name,
		       mat_service_version AS version,
		       mat_deployment_environment AS environment,
		       min(s.timestamp) AS first_seen,
		       max(s.timestamp) AS last_seen,
		       count() AS span_count,
		       `+commitMetaSelect+`
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
	return toDeploymentAggRows(rows), err
}

func (r *ClickHouseRepository) ListServiceDeployments(ctx context.Context, teamID int64, serviceName string) ([]deploymentAggRow, error) {
	var rows []deploymentAggScanRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT s.service_name AS service_name,
		       s.mat_service_version AS version,
		       s.mat_deployment_environment AS environment,
		       min(s.timestamp) AS first_seen,
		       max(s.timestamp) AS last_seen,
		       count() AS span_count,
		       `+commitMetaSelect+`
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
	return toDeploymentAggRows(rows), err
}

func (r *ClickHouseRepository) GetLatestDeploymentsByService(ctx context.Context, teamID int64) ([]deploymentAggRow, error) {
	var rows []deploymentAggScanRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
		WITH deployments AS (
			SELECT s.service_name AS service_name,
			       s.mat_service_version AS version,
			       s.mat_deployment_environment AS environment,
			       min(s.timestamp) AS first_seen,
			       max(s.timestamp) AS last_seen,
			       count() AS span_count,
			       `+commitMetaSelect+`
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
		       deployments.span_count AS span_count,
		       deployments.commit_sha AS commit_sha,
		       deployments.commit_author AS commit_author,
		       deployments.repo_url AS repo_url,
		       deployments.pr_url AS pr_url
		FROM deployments
		INNER JOIN latest
		  ON deployments.service_name = latest.service_name
		 AND deployments.first_seen = latest.max_first_seen
		ORDER BY deployments.service_name ASC
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
	)
	return toDeploymentAggRows(rows), err
}

// GetDeploysInRange returns all deploys (root-span markers) in [startMs,endMs]
// across every service for a team. Used by alerting for fire/resolve deploy
// correlation.
func (r *ClickHouseRepository) GetDeploysInRange(ctx context.Context, teamID int64, startMs, endMs int64) ([]deploymentAggRow, error) {
	var rows []deploymentAggScanRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT s.service_name AS service_name,
		       s.mat_service_version AS version,
		       s.mat_deployment_environment AS environment,
		       min(s.timestamp) AS first_seen,
		       max(s.timestamp) AS last_seen,
		       count() AS span_count,
		       `+commitMetaSelect+`
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
	return toDeploymentAggRows(rows), err
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
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query,
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

// --- Impact window ----------------------------------------------------------
//
// Previously the single SELECT produced both request and error counts via
// banned CH casts and conditional-count combinators. Two narrow parallel
// scans now produce counts and the service overlays p95/p99 from the
// SpanLatencyService sketch (unchanged).

type impactTotalRow struct {
	RequestCount uint64 `ch:"request_count"`
}

type impactErrorRow struct {
	ErrorCount uint64 `ch:"error_count"`
}

func (r *ClickHouseRepository) GetImpactWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (impactAggRow, error) {
	if endMs <= startMs {
		return impactAggRow{}, nil
	}

	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var (
		totalRow impactTotalRow
		errorRow impactErrorRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), `
			SELECT count() AS request_count
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND s.service_name = @serviceName
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp >= @start AND s.timestamp < @end
			  AND `+rootspan.Condition("s")+`
			  AND s.mat_service_version != ''
		`, args...).ScanStruct(&totalRow)
	})
	g.Go(func() error {
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), `
			SELECT count() AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND s.service_name = @serviceName
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp >= @start AND s.timestamp < @end
			  AND `+rootspan.Condition("s")+`
			  AND s.mat_service_version != ''
			  AND (s.has_error = true OR s.http_status_code >= 400)
		`, args...).ScanStruct(&errorRow)
	})
	if err := g.Wait(); err != nil {
		return impactAggRow{}, err
	}

	return impactAggRow{
		RequestCount: int64(totalRow.RequestCount), //nolint:gosec
		ErrorCount:   int64(errorRow.ErrorCount),   //nolint:gosec
	}, nil
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

// errorGroupScanRow is the scan shape before the service converts to the
// public errorGroupAggRow. response_status_code is LowCardinality(String)
// natively, and the schema ALIAS `http_status_code` parses it to UInt16;
// we reference the alias so user-written SQL stays cast-combinator-free.
type errorGroupScanRow struct {
	ServiceName    string    `ch:"service_name"`
	GroupID        string    `ch:"group_id"`
	OperationName  string    `ch:"operation_name"`
	StatusMessage  string    `ch:"status_message"`
	HTTPStatusCode uint16    `ch:"http_status_code"`
	ErrorCount     uint64    `ch:"error_count"`
	LastOccurrence time.Time `ch:"last_occurrence"`
	SampleTraceID  string    `ch:"sample_trace_id"`
}

func (r *ClickHouseRepository) GetErrorGroupsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]errorGroupAggRow, error) {
	var scanRows []errorGroupScanRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &scanRows, `
		SELECT s.service_name AS service_name,
		       '' AS group_id,
		       s.name AS operation_name,
		       s.status_message AS status_message,
		       s.http_status_code AS http_status_code,
		       count() AS error_count,
		       max(s.timestamp) AS last_occurrence,
		       argMax(s.trace_id, s.timestamp) AS sample_trace_id
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.service_name = @serviceName
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp >= @start AND s.timestamp < @end
		  AND (s.has_error = true OR s.http_status_code >= 400)
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
	if err != nil {
		return nil, err
	}
	out := make([]errorGroupAggRow, len(scanRows))
	for i, r := range scanRows {
		out[i] = errorGroupAggRow{
			ServiceName:    r.ServiceName,
			GroupID:        r.GroupID,
			OperationName:  r.OperationName,
			StatusMessage:  r.StatusMessage,
			HTTPStatusCode: int(r.HTTPStatusCode),
			ErrorCount:     r.ErrorCount,
			LastOccurrence: r.LastOccurrence,
			SampleTraceID:  r.SampleTraceID,
		}
	}
	return out, nil
}

// --- Endpoint metrics window ------------------------------------------------
//
// Previously one SELECT produced request_count / error_count / p95_ms / p99_ms
// using banned CH casts, conditional-count combinators, and a null-fallback
// chain. New shape:
//   - repository groups by (operation_name, mat_http_route, mat_http_target,
//     http_method) and returns raw request counts in one scan and error
//     counts in a second parallel scan;
//   - the service resolves the public endpoint_name in Go with the same
//     `route | target | operation` precedence previously encoded in the
//     null-fallback chain;
//   - percentiles come from SpanLatencyEndpoint via sketch.Querier (already
//     wired in service.go).

type endpointTotalRow struct {
	OperationName string `ch:"operation_name"`
	HTTPRoute     string `ch:"mat_http_route"`
	HTTPTarget    string `ch:"mat_http_target"`
	HTTPMethod    string `ch:"http_method"`
	RequestCount  uint64 `ch:"request_count"`
}

type endpointErrorRow struct {
	OperationName string `ch:"operation_name"`
	HTTPRoute     string `ch:"mat_http_route"`
	HTTPTarget    string `ch:"mat_http_target"`
	HTTPMethod    string `ch:"http_method"`
	ErrorCount    uint64 `ch:"error_count"`
}

func (r *ClickHouseRepository) GetEndpointMetricsWindow(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, limit int) ([]endpointMetricAggRow, error) {
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	}

	var (
		totals []endpointTotalRow
		errs   []endpointErrorRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, `
			SELECT s.name AS operation_name,
			       s.mat_http_route AS mat_http_route,
			       s.mat_http_target AS mat_http_target,
			       s.http_method AS http_method,
			       count() AS request_count
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND s.service_name = @serviceName
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp >= @start AND s.timestamp < @end
			  AND `+rootspan.Condition("s")+`
			GROUP BY operation_name, mat_http_route, mat_http_target, http_method
			ORDER BY request_count DESC
			LIMIT @limit
		`, args...)
	})
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, `
			SELECT s.name AS operation_name,
			       s.mat_http_route AS mat_http_route,
			       s.mat_http_target AS mat_http_target,
			       s.http_method AS http_method,
			       count() AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND s.service_name = @serviceName
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp >= @start AND s.timestamp < @end
			  AND `+rootspan.Condition("s")+`
			  AND (s.has_error = true OR s.http_status_code >= 400)
			GROUP BY operation_name, mat_http_route, mat_http_target, http_method
		`, args...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	type key struct {
		operation string
		route     string
		target    string
		method    string
	}
	errIdx := make(map[key]uint64, len(errs))
	for _, e := range errs {
		errIdx[key{operation: e.OperationName, route: e.HTTPRoute, target: e.HTTPTarget, method: e.HTTPMethod}] = e.ErrorCount
	}

	out := make([]endpointMetricAggRow, 0, len(totals))
	for _, t := range totals {
		errCount := errIdx[key{operation: t.OperationName, route: t.HTTPRoute, target: t.HTTPTarget, method: t.HTTPMethod}]
		out = append(out, endpointMetricAggRow{
			OperationName: t.OperationName,
			EndpointName:  resolveEndpointName(t.HTTPRoute, t.HTTPTarget, t.OperationName),
			HTTPMethod:    t.HTTPMethod,
			RequestCount:  int64(t.RequestCount), //nolint:gosec
			ErrorCount:    int64(errCount),       //nolint:gosec
		})
	}
	return out, nil
}

// resolveEndpointName replaces the banned null-fallback chain over
// (mat_http_route, mat_http_target, name): prefer the route, then target,
// then fall back to the span name.
func resolveEndpointName(httpRoute, httpTarget, operationName string) string {
	if httpRoute != "" {
		return httpRoute
	}
	if httpTarget != "" {
		return httpTarget
	}
	return operationName
}
