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

// GetNodes returns per-service RED aggregates plus a fixed-bucket latency
// histogram. Percentiles are interpolated Go-side from bucket_counts via
// quantile.FromHistogram — no quantile aggregate runs in CH.
func (r *ClickHouseRepository) GetNodes(ctx context.Context, teamID, startMs, endMs int64) ([]nodeAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service                                                                       AS service,
		       toInt64(count())                                                              AS request_count,
		       toInt64(countIf(has_error OR toUInt16OrZero(response_status_code) >= 400))    AS error_count,
		       [
		           countIf(duration_nano <  5000000),
		           countIf(duration_nano >=  5000000 AND duration_nano <  10000000),
		           countIf(duration_nano >= 10000000 AND duration_nano <  25000000),
		           countIf(duration_nano >= 25000000 AND duration_nano <  50000000),
		           countIf(duration_nano >= 50000000 AND duration_nano < 100000000),
		           countIf(duration_nano >= 100000000 AND duration_nano < 250000000),
		           countIf(duration_nano >= 250000000 AND duration_nano < 500000000),
		           countIf(duration_nano >= 500000000 AND duration_nano < 1000000000),
		           countIf(duration_nano >= 1000000000 AND duration_nano < 2500000000),
		           countIf(duration_nano >= 2500000000 AND duration_nano < 5000000000),
		           countIf(duration_nano >= 5000000000 AND duration_nano < 10000000000),
		           countIf(duration_nano >= 10000000000 AND duration_nano < 30000000000),
		           countIf(duration_nano >= 30000000000)
		       ]                                                                             AS bucket_counts
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service != ''
		GROUP BY service`
	var rows []nodeAggRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "topology.GetNodes", &rows, query, spanArgs(teamID, startMs, endMs)...)
}

// GetEdges derives directed edges from client-kind spans: every Client span
// where peer_service is set yields a (service → peer_service) call. Same
// histogram-bucket emission as GetNodes; service computes percentiles.
func (r *ClickHouseRepository) GetEdges(ctx context.Context, teamID, startMs, endMs int64) ([]edgeAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT service                                                                       AS source,
		       peer_service                                                                  AS target,
		       toInt64(count())                                                              AS call_count,
		       toInt64(countIf(has_error OR toUInt16OrZero(response_status_code) >= 400))    AS error_count,
		       [
		           countIf(duration_nano <  5000000),
		           countIf(duration_nano >=  5000000 AND duration_nano <  10000000),
		           countIf(duration_nano >= 10000000 AND duration_nano <  25000000),
		           countIf(duration_nano >= 25000000 AND duration_nano <  50000000),
		           countIf(duration_nano >= 50000000 AND duration_nano < 100000000),
		           countIf(duration_nano >= 100000000 AND duration_nano < 250000000),
		           countIf(duration_nano >= 250000000 AND duration_nano < 500000000),
		           countIf(duration_nano >= 500000000 AND duration_nano < 1000000000),
		           countIf(duration_nano >= 1000000000 AND duration_nano < 2500000000),
		           countIf(duration_nano >= 2500000000 AND duration_nano < 5000000000),
		           countIf(duration_nano >= 5000000000 AND duration_nano < 10000000000),
		           countIf(duration_nano >= 10000000000 AND duration_nano < 30000000000),
		           countIf(duration_nano >= 30000000000)
		       ]                                                                             AS bucket_counts
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND kind_string  = 'Client'
		  AND service      != ''
		  AND peer_service != ''
		  AND service      != peer_service
		GROUP BY source, target`
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
