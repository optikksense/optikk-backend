package nodes

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/shared/chargs"
)

// Repository reads observability.spans for per-host RED aggregates.

// Query limits and defaults for node aggregates.
const (
	MaxNodes       = 200
	MaxServices    = 100
	DefaultUnknown = "unknown"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

func (r *Repository) QueryInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]NodeAggregateRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT
		    if(host != '', host, @defaultUnknown)                                                AS host,
		    uniqIf(pod, pod != '')                                                               AS pod_count,
		    sum(request_count)                                                                   AS request_count,
		    sum(error_count)                                                                     AS error_count,
		    sum(duration_ms_sum)                                                                 AS duration_ms_sum,
		    quantileTimingMerge(0.95)(latency_state)                                             AS p95_latency_ms,
		    max(timestamp)                                                                       AS last_seen
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY host
		ORDER BY request_count DESC
		LIMIT @maxNodes`
	args := chargs.RangeArgs(teamID, startMs, endMs)
	args = append(args,
		clickhouse.Named("defaultUnknown", DefaultUnknown),
		clickhouse.Named("maxNodes", uint64(MaxNodes)),
	)
	var rows []NodeAggregateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "nodes.QueryInfrastructureNodes", &rows, query, args...)
}

func (r *Repository) QueryInfrastructureNodeSummary(ctx context.Context, teamID int64, startMs, endMs int64) (NodeSummaryRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT
		    host,
		    sum(error_count)      AS error_count,
		    sum(request_count)    AS request_count,
		    uniqIf(pod, pod != '') AS pod_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY host`
	args := chargs.RangeArgs(teamID, startMs, endMs)
	type nodeRawSummaryRow struct {
		Host         string `ch:"host"`
		ErrorCount   uint64 `ch:"error_count"`
		RequestCount uint64 `ch:"request_count"`
		PodCount     uint64 `ch:"pod_count"`
	}
	var rawRows []nodeRawSummaryRow
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "nodes.QueryInfrastructureNodeSummary", &rawRows, query, args...); err != nil {
		return NodeSummaryRow{}, err
	}

	var healthy, degraded, unhealthy, totalPods uint64
	for _, raw := range rawRows {
		totalPods += raw.PodCount
		var errorRate float64
		if raw.RequestCount > 0 {
			errorRate = float64(raw.ErrorCount) * 100.0 / float64(raw.RequestCount)
		}
		switch {
		case errorRate > 10:
			unhealthy++
		case errorRate > 2 && errorRate <= 10:
			degraded++
		default:
			healthy++
		}
	}

	return NodeSummaryRow{
		HealthyNodes:   healthy,
		DegradedNodes:  degraded,
		UnhealthyNodes: unhealthy,
		TotalPods:      &totalPods,
	}, nil
}

func (r *Repository) QueryInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]NodeServiceAggregateRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT
		    service                                                                              AS service,
		    sum(request_count)                                                                   AS request_count,
		    sum(error_count)                                                                     AS error_count,
		    sum(duration_ms_sum)                                                                 AS duration_ms_sum,
		    quantileTimingMerge(0.95)(latency_state)                                             AS p95_latency_ms,
		    uniqIf(pod, pod != '')                                                               AS pod_count
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND if(host != '', host, @defaultUnknown) = @host
		GROUP BY service
		ORDER BY request_count DESC
		LIMIT @maxServices`
	args := chargs.RangeArgs(teamID, startMs, endMs)
	args = append(args,
		clickhouse.Named("host", host),
		clickhouse.Named("defaultUnknown", DefaultUnknown),
		clickhouse.Named("maxServices", uint64(MaxServices)),
	)
	var rows []NodeServiceAggregateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "nodes.QueryInfrastructureNodeServices", &rows, query, args...)
}
