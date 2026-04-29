package latency

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// heatmapBucketWidthMs is the linear bucket width for the heatmap. Picked
// for parity with the prior rollup-driven cell shape; raw-span scan groups
// by floor(ms / width).
const heatmapBucketWidthMs = 50

type Repository struct{ db clickhouse.Conn }

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

func (r *Repository) Histogram(ctx context.Context, teamID int64, req HistogramRequest) (histogramRow, error) {
	args := spanArgs(teamID, req.StartMs, req.EndMs)
	resourceWhere := ""
	if req.ServiceName != "" {
		resourceWhere = ` AND service = @service`
		args = append(args, clickhouse.Named("service", req.ServiceName))
	}
	where := ""
	if req.Operation != "" {
		where = ` AND name = @op`
		args = append(args, clickhouse.Named("op", req.Operation))
	}
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT quantileTiming(0.5) (duration_nano / 1000000.0)                  AS p50,
		       quantileTiming(0.9) (duration_nano / 1000000.0)                  AS p90,
		       quantileTiming(0.95)(duration_nano / 1000000.0)                  AS p95,
		       quantileTiming(0.99)(duration_nano / 1000000.0)                  AS p99,
		       max(duration_nano / 1000000.0)                                   AS max,
		       avg(duration_nano / 1000000.0)                                   AS avg
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end` + where
	var out histogramRow
	return out, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "latency.Histogram", &out, query, args...)
}

func (r *Repository) Heatmap(ctx context.Context, teamID int64, req HeatmapRequest) ([]heatmapRow, error) {
	args := spanArgs(teamID, req.StartMs, req.EndMs)
	args = append(args, clickhouse.Named("bucketWidthMs", uint64(heatmapBucketWidthMs)))
	resourceWhere := ""
	if req.ServiceName != "" {
		resourceWhere = ` AND service = @service`
		args = append(args, clickhouse.Named("service", req.ServiceName))
	}
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT toString(ts_bucket)                                                            AS time_bucket,
		       toFloat64(floor(duration_nano / 1000000.0 / @bucketWidthMs) * @bucketWidthMs)  AS bucket_ms,
		       count()                                                                        AS count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY time_bucket, bucket_ms
		ORDER BY time_bucket ASC, bucket_ms ASC
		LIMIT 5000`
	var rows []heatmapRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "latency.Heatmap", &rows, query, args...)
}

func spanArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := spanBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — TeamID fits UInt32
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
