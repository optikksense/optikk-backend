package topology

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// Repository runs ClickHouse queries that power the runtime service topology.
// Queries only — all derivation (percentile interpolation, error-rate, health
// classification) lives in service.go.
type Repository interface {
	GetNodes(ctx context.Context, teamID, startMs, endMs int64, focusService string) ([]nodeAggRow, error)
	GetEdges(ctx context.Context, teamID, startMs, endMs int64, focusService string) ([]edgeAggRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetNodes returns per-service RED aggregates plus p50/p95/p99 latency,
// computed via quantileTimingMerge on spans_1m's latency_state.
func (r *ClickHouseRepository) GetNodes(ctx context.Context, teamID, startMs, endMs int64, focusService string) ([]nodeAggRow, error) {
	const query = `
		SELECT service                                                AS service,
		       sum(request_count)                                     AS request_count,
		       sum(error_count)                                       AS error_count,
		       quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state)   AS qs
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND service != ''
		  AND (
		      @focusService = '' OR
		      service = @focusService OR
		      service IN (
		          SELECT DISTINCT peer_service 
		          FROM observability.spans_1m 
		          WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND service = @focusService AND kind_string = 'Client'
		      ) OR
		      service IN (
		          SELECT DISTINCT service 
		          FROM observability.spans_1m 
		          WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND peer_service = @focusService AND kind_string = 'Client'
		      )
		  )
		GROUP BY service`
	var rows []nodeAggRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "topology.GetNodes", &rows, query, spanArgs(teamID, startMs, endMs, focusService)...); err != nil {
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

// GetEdges derives directed edges from client-kind spans: every Client span
// where peer_service is set yields a (service → peer_service) call. Same
// quantileTimingMerge shape as GetNodes; only p50/p95 are needed for edges.
func (r *ClickHouseRepository) GetEdges(ctx context.Context, teamID, startMs, endMs int64, focusService string) ([]edgeAggRow, error) {
	const query = `
		SELECT service                                          AS source,
		       peer_service                                     AS target,
		       sum(request_count)                               AS call_count,
		       sum(error_count)                                 AS error_count,
		       quantilesTimingMerge(0.5, 0.95)(latency_state)   AS qs
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND kind_string  = 'Client'
		  AND service      != ''
		  AND peer_service != ''
		  AND service      != peer_service
		  AND (@focusService = '' OR service = @focusService OR peer_service = @focusService)
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

// spanArgs binds the 6 parameters every topology query needs: team scope,
// 5-minute-aligned ts_bucket bounds for PREWHERE, millisecond timestamp
// bounds for the row-side WHERE, and focused service filter.
func spanArgs(teamID, startMs, endMs int64, focusService string) []any {
	bucketStart, bucketEnd := spanBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("focusService", focusService),
	}
}

// spanBucketBounds returns the 5-minute-aligned [bucketStart, bucketEnd)
// covering [startMs, endMs] in spans_resource / spans PK terms. Same shape
// as services/latency.spanBucketBounds.
func spanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
