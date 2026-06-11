package fleet

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/shared/chargs"
)

const (
	maxFleetPods   = 200
	defaultUnknown = "unknown"
)

// fleet reads spans for per-pod RED aggregates using spans_resource.

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

func (r *Repository) QueryFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]FleetPodAggregateRow, error) {
	query := `
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
		FROM ` + timebucket.SpansRollup(endMs-startMs) + `
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND pod != ''
		GROUP BY pod, host
		ORDER BY request_count DESC
		LIMIT @maxFleetPods`
	args := chargs.RollupRangeArgs(teamID, startMs, endMs)
	args = append(args,
		clickhouse.Named("defaultUnknown", defaultUnknown),
		clickhouse.Named("maxFleetPods", uint64(maxFleetPods)),
	)
	var rows []FleetPodAggregateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "fleet.QueryFleetPods", &rows, query, args...)
}
