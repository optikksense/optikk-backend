package latency

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
)

const spansLatencyRollupPrefix = rollup.FamilySpansLatency

type Repository struct{ db clickhouse.Conn }

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

func (r *Repository) Histogram(ctx context.Context, teamID int64, req HistogramRequest) (histogramRow, error) {
	table := rollup.For(spansLatencyRollupPrefix, req.StartMs, req.EndMs).Table
	where, args := rollupFilter(teamID, req.StartMs, req.EndMs, req.ServiceName)
	if req.Operation != "" {
		where += ` AND operation_name = @op`
		args = append(args, clickhouse.Named("op", req.Operation))
	}
	query := fmt.Sprintf(`
		SELECT
			toFloat64(quantilesTDigestWeightedMerge(0.5, 0.9, 0.95, 0.99)(latency_ms_digest)[1]) AS p50,
			toFloat64(quantilesTDigestWeightedMerge(0.5, 0.9, 0.95, 0.99)(latency_ms_digest)[2]) AS p90,
			toFloat64(quantilesTDigestWeightedMerge(0.5, 0.9, 0.95, 0.99)(latency_ms_digest)[3]) AS p95,
			toFloat64(quantilesTDigestWeightedMerge(0.5, 0.9, 0.95, 0.99)(latency_ms_digest)[4]) AS p99,
			maxMerge(max_latency_ms) AS max,
			if(sumMerge(span_count) > 0, sumMerge(duration_ms_sum) / toFloat64(sumMerge(span_count)), 0.0) AS avg
		FROM %s
		WHERE %s`, table, where)
	var out histogramRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&out); err != nil {
		return histogramRow{}, err
	}
	return out, nil
}

func (r *Repository) Heatmap(ctx context.Context, teamID int64, req HeatmapRequest) ([]heatmapRow, error) {
	tier := rollup.For(spansLatencyRollupPrefix, req.StartMs, req.EndMs)
	table, tierStep := tier.Table, tier.StepMin
	where, args := rollupFilter(teamID, req.StartMs, req.EndMs, req.ServiceName)
	args = append(args, clickhouse.Named("intervalMin", adaptiveIntervalMinutes(tierStep, req.StartMs, req.EndMs)))
	query := fmt.Sprintf(`
		SELECT toString(toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin))) AS time_bucket,
		       latency_bucket AS bucket_ms,
		       sumMerge(span_count) AS count
		FROM %s
		WHERE %s
		GROUP BY time_bucket, bucket_ms
		ORDER BY time_bucket ASC, bucket_ms ASC
		LIMIT 5000`, table, where)
	var rows []heatmapRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "latency.Heatmap", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func rollupFilter(teamID int64, startMs, endMs int64, service string) (string, []any) {
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // domain-bounded
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	where := `team_id = @teamID AND bucket_ts BETWEEN @start AND @end`
	if service != "" {
		where += ` AND service_name = @service`
		args = append(args, clickhouse.Named("service", service))
	}
	return where, args
}

func adaptiveIntervalMinutes(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var uiStep int64
	switch {
	case hours <= 3:
		uiStep = 1
	case hours <= 24:
		uiStep = 5
	case hours <= 168:
		uiStep = 60
	default:
		uiStep = 1440
	}
	return rollup.BucketInterval(rollup.Tier{StepMin: tierStepMin}, uiStep)
}
