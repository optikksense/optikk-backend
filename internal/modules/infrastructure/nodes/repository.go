package nodes

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// nodes reads `observability.spans` for per-host RED aggregates. Mirrors
// httpmetrics' route/external-host queries: `WITH active_fps AS (SELECT
// DISTINCT fingerprint FROM observability.spans_resource WHERE team_id +
// ts_bucket)` CTE so the main spans scan PREWHEREs on (team_id, ts_bucket,
// fingerprint) — first three slots of the spans PK.

type Repository interface {
	QueryInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]NodeAggregateRow, error)
	QueryInfrastructureNodeSummary(ctx context.Context, teamID int64, startMs, endMs int64) (NodeSummaryRow, error)
	QueryInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]NodeServiceAggregateRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) QueryInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]NodeAggregateRow, error) {
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
		    count()                                                                              AS request_count,
		    countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                    AS error_count,
		    sum(duration_nano / 1000000.0)                                                       AS duration_ms_sum,
		    quantileTiming(0.95)(duration_nano / 1000000.0)                                      AS p95_latency_ms,
		    max(timestamp)                                                                       AS last_seen
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY host
		ORDER BY request_count DESC
		LIMIT @maxNodes`
	args := spanArgs(teamID, startMs, endMs)
	args = append(args,
		clickhouse.Named("defaultUnknown", DefaultUnknown),
		clickhouse.Named("maxNodes", uint64(MaxNodes)),
	)
	var rows []NodeAggregateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "nodes.QueryInfrastructureNodes", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryInfrastructureNodeSummary(ctx context.Context, teamID int64, startMs, endMs int64) (NodeSummaryRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT
		    toInt64(countIf(error_rate > 10))                        AS unhealthy_nodes,
		    toInt64(countIf(error_rate > 2 AND error_rate <= 10))    AS degraded_nodes,
		    toInt64(countIf(error_rate <= 2))                        AS healthy_nodes,
		    toInt64(sum(pod_count))                                  AS total_pods
		FROM (
		    SELECT
		        host,
		        (countIf(has_error OR toUInt16OrZero(response_status_code) >= 400) * 100.0
		            / nullIf(toFloat64(count()), 0))                 AS error_rate,
		        uniqIf(pod, pod != '')                               AS pod_count
		    FROM observability.spans
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND fingerprint IN active_fps
		    WHERE timestamp BETWEEN @start AND @end
		    GROUP BY host
		)`
	args := spanArgs(teamID, startMs, endMs)
	var row NodeSummaryRow
	return row, dbutil.QueryRowCH(dbutil.DashboardCtx(ctx), r.db, "nodes.QueryInfrastructureNodeSummary", &row, query, args...)
}

func (r *ClickHouseRepository) QueryInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]NodeServiceAggregateRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT
		    service                                                                              AS service,
		    count()                                                                              AS request_count,
		    countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                    AS error_count,
		    sum(duration_nano / 1000000.0)                                                       AS duration_ms_sum,
		    quantileTiming(0.95)(duration_nano / 1000000.0)                                      AS p95_latency_ms,
		    uniqIf(pod, pod != '')                                                               AS pod_count
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND if(host != '', host, @defaultUnknown) = @host
		GROUP BY service
		ORDER BY request_count DESC
		LIMIT @maxServices`
	args := spanArgs(teamID, startMs, endMs)
	args = append(args,
		clickhouse.Named("host", host),
		clickhouse.Named("defaultUnknown", DefaultUnknown),
		clickhouse.Named("maxServices", uint64(MaxServices)),
	)
	var rows []NodeServiceAggregateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "nodes.QueryInfrastructureNodeServices", &rows, query, args...)
}

// ---------------------------------------------------------------------------
// Local helpers — each module owns its own.
// ---------------------------------------------------------------------------

func spanArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := spanBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func spanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
