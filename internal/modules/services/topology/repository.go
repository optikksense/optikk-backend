package topology

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// Repository runs ClickHouse queries that power the runtime service topology.
// All derivation logic lives in service.go.
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

// GetNodes returns per-service RED aggregates and p50/p95/p99 latency.
func (r *ClickHouseRepository) GetNodes(ctx context.Context, teamID, startMs, endMs int64, _ string) ([]nodeAggRow, error) {
	const query = `
		SELECT service                                                AS service,
		       sum(request_count)                                     AS request_count,
		       sum(error_count)                                       AS error_count,
		       quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state)   AS qs
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @start AND @end
		  AND service != ''
		GROUP BY service`
	var rows []nodeAggRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "topology.GetNodes", &rows, query, baseArgs(teamID, startMs, endMs)...); err != nil {
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
func (r *ClickHouseRepository) GetEdges(ctx context.Context, teamID, startMs, endMs int64, focusService string) ([]edgeAggRow, error) {
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

// baseArgs binds the parameters needed for topology queries.
func baseArgs(teamID, startMs, endMs int64) []any {
	bucketStart, bucketEnd := spanBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// spanArgs is baseArgs plus the focused-service filter bound for GetEdges.
func spanArgs(teamID, startMs, endMs int64, focusService string) []any {
	return append(baseArgs(teamID, startMs, endMs), clickhouse.Named("focusService", focusService))
}

// spanBucketBounds returns bucket bounds covering [startMs, endMs].
func spanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
