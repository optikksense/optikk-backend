package topology

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/shared/chargs"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

// GetNodes returns per-service RED aggregates and p50/p95/p99 latency.
func (r *Repository) GetNodes(ctx context.Context, teamID, startMs, endMs int64, _ string) ([]nodeAggRow, error) {
	query := `
		SELECT service                                                AS service,
		       sum(request_count)                                     AS request_count,
		       sum(error_count)                                       AS error_count,
		       quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state)   AS qs
		FROM ` + timebucket.SpansRollup(endMs-startMs) + `
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND service != ''
		GROUP BY service`
	var rows []nodeAggRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "topology.GetNodes", &rows, query, chargs.RollupRangeArgs(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	for i := range rows {
		if len(rows[i].QS) >= 3 {
			rows[i].P50Ms = rows[i].QS[0]
			rows[i].P95Ms = rows[i].QS[1]
			rows[i].P99Ms = rows[i].QS[2]
		}
	}
	return rows, nil
}

// GetEdges derives directed edges from parent-child span links.
func (r *Repository) GetEdges(ctx context.Context, teamID, startMs, endMs int64, focusService string) ([]edgeAggRow, error) {
	const query = `
		SELECT p.service                                                              AS source,
		       c.service                                                              AS target,
		       count()                                                                AS call_count,
		       countIf(c.has_error OR toUInt16OrZero(c.response_status_code) >= 400)   AS error_count,
		       quantilesTiming(0.5, 0.95)(c.duration_nano / 1000000.0)                AS qs
		FROM observability.spans AS c
		INNER JOIN observability.spans AS p
		  ON c.team_id = p.team_id AND c.trace_id = p.trace_id AND c.parent_span_id = p.span_id
		WHERE c.team_id = @teamID
		  AND c.ts_bucket BETWEEN @bucketStart AND @bucketEnd
		  AND p.ts_bucket BETWEEN @bucketStart AND @bucketEnd
		  AND c.timestamp BETWEEN @start AND @end
		  AND c.service != ''
		  AND p.service != ''
		  AND c.service != p.service
		  AND (@focusService = '' OR p.service = @focusService OR c.service = @focusService)
		GROUP BY source, target`
	var rows []edgeAggRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "topology.GetEdges", &rows, query, spanArgs(teamID, startMs, endMs, focusService)...); err != nil {
		return nil, err
	}
	for i := range rows {
		if len(rows[i].QS) >= 2 {
			rows[i].P50Ms = rows[i].QS[0]
			rows[i].P95Ms = rows[i].QS[1]
		}
	}
	return rows, nil
}

// spanArgs is baseArgs plus the focused-service filter bound for GetEdges.
func spanArgs(teamID, startMs, endMs int64, focusService string) []any {
	return append(chargs.RangeArgs(teamID, startMs, endMs), clickhouse.Named("focusService", focusService))
}
