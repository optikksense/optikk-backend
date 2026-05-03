package fleet

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

const (
	maxFleetPods   = 200
	defaultUnknown = "unknown"
)

// fleet reads `observability.spans` for per-pod RED aggregates with the same
// `WITH active_fps AS (... spans_resource ...)` CTE httpmetrics uses for its
// route/external-host queries. Service derives error_rate / avg_latency_ms.

type Repository interface {
	QueryFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]FleetPodAggregateRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) QueryFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]FleetPodAggregateRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT
		    pod                                                                                  AS pod,
		    if(host != '', host, @defaultUnknown)                                                AS host,
		    groupUniqArray(service)                                                              AS services,
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
		  AND pod != ''
		GROUP BY pod, host
		ORDER BY request_count DESC
		LIMIT @maxFleetPods`
	args := spanArgs(teamID, startMs, endMs)
	args = append(args,
		clickhouse.Named("defaultUnknown", defaultUnknown),
		clickhouse.Named("maxFleetPods", uint64(maxFleetPods)),
	)
	var rows []FleetPodAggregateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "fleet.QueryFleetPods", &rows, query, args...)
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
