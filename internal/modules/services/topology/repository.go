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
// classification, neighborhood filtering) lives in service.go.
type Repository interface {
	GetNodes(ctx context.Context, teamID, startMs, endMs int64) ([]nodeAggRow, error)
	GetEdges(ctx context.Context, teamID, startMs, endMs int64) ([]edgeAggRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetNodes returns per-service RED aggregates plus p50/p95/p99 latency,
// computed via quantileTimingMerge on spans_1m's latency_state.
func (r *ClickHouseRepository) GetNodes(ctx context.Context, teamID, startMs, endMs int64) ([]nodeAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service,
		       request_count,
		       error_count,
		       qs[1] AS p50_ms,
		       qs[2] AS p95_ms,
		       qs[3] AS p99_ms
		FROM (
		    SELECT service                                                AS service,
		           toInt64(sum(request_count))                            AS request_count,
		           toInt64(sum(error_count))                              AS error_count,
		           quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state)   AS qs
		    FROM observability.spans_1m
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		    WHERE timestamp BETWEEN @start AND @end
		      AND service != ''
		    GROUP BY service
		)`
	var rows []nodeAggRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "topology.GetNodes", &rows, query, spanArgs(teamID, startMs, endMs)...)
}

// GetEdges derives directed edges from client-kind spans: every Client span
// where peer_service is set yields a (service → peer_service) call. Same
// quantileTimingMerge shape as GetNodes; only p50/p95 are needed for edges.
func (r *ClickHouseRepository) GetEdges(ctx context.Context, teamID, startMs, endMs int64) ([]edgeAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT source,
		       target,
		       call_count,
		       error_count,
		       qs[1] AS p50_ms,
		       qs[2] AS p95_ms
		FROM (
		    SELECT service                                          AS source,
		           peer_service                                     AS target,
		           toInt64(sum(request_count))                      AS call_count,
		           toInt64(sum(error_count))                        AS error_count,
		           quantilesTimingMerge(0.5, 0.95)(latency_state)   AS qs
		    FROM observability.spans_1m
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		    WHERE timestamp BETWEEN @start AND @end
		      AND kind_string  = 'Client'
		      AND service      != ''
		      AND peer_service != ''
		      AND service      != peer_service
		    GROUP BY source, target
		)`
	var rows []edgeAggRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "topology.GetEdges", &rows, query, spanArgs(teamID, startMs, endMs)...)
}

// spanArgs binds the 5 parameters every topology query needs: team scope,
// 5-minute-aligned ts_bucket bounds for PREWHERE, and millisecond timestamp
// bounds for the row-side WHERE.
func spanArgs(teamID, startMs, endMs int64) []any {
	bucketStart, bucketEnd := spanBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// spanBucketBounds returns the 5-minute-aligned [bucketStart, bucketEnd)
// covering [startMs, endMs] in spans_resource / spans PK terms. Same shape
// as services/latency.spanBucketBounds.
func spanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
