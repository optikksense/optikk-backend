package latency

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetLatencyBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error)
	GetLatencyByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error)
	GetLatencyByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error)
	GetLatencyByNamespace(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error)
	GetLatencyByServer(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error)
	GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyHeatmapBucket, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) latencySeriesByAttr(ctx context.Context, teamID int64, startMs, endMs int64, groupAttr string, f shared.Filters) ([]LatencyTimeSeries, error) {
	tier := rollup.For(shared.DBHistRollupPrefix, startMs, endMs)
	table, tierStep := tier.Table, tier.StepMin
	fc, fargs := shared.RollupFilterClauses(f)
	groupCol := shared.GroupColumnFor(groupAttr)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    %s                                                                          AS group_by,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1]) * 1000  AS p50_ms,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) * 1000  AS p95_ms,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) * 1000  AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, groupCol, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)
	var rows []LatencyTimeSeries
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "latency.latencySeriesByAttr", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetLatencyBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBSystem, f)
}

func (r *ClickHouseRepository) GetLatencyByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBOperationName, f)
}

func (r *ClickHouseRepository) GetLatencyByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBCollectionName, f)
}

func (r *ClickHouseRepository) GetLatencyByNamespace(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBNamespace, f)
}

func (r *ClickHouseRepository) GetLatencyByServer(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrServerAddress, f)
}

// GetLatencyHeatmap bins bucket-level avg latency (hist_sum/hist_count) into
// coarse latency ranges. The rollup state loses per-row latency, so we
// approximate at the tier's native bucket granularity — accurate at rollup
// resolution, coarser than the old per-row query but preserves the chart.
func (r *ClickHouseRepository) GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyHeatmapBucket, error) {
	tier := rollup.For(shared.DBHistRollupPrefix, startMs, endMs)
	table, tierStep := tier.Table, tier.StepMin
	fc, fargs := shared.RollupFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    time_bucket,
		    multiIf(
		        avg_sec < 0.001,  '< 1ms',
		        avg_sec < 0.005,  '1-5ms',
		        avg_sec < 0.010,  '5-10ms',
		        avg_sec < 0.025,  '10-25ms',
		        avg_sec < 0.050,  '25-50ms',
		        avg_sec < 0.100,  '50-100ms',
		        avg_sec < 0.250,  '100-250ms',
		        avg_sec < 0.500,  '250-500ms',
		        avg_sec < 1.000,  '500ms-1s',
		        '> 1s'
		    )                                                         AS bucket_label,
		    toInt64(sum(hc))                                          AS count
		FROM (
		    SELECT
		        %s                                                        AS time_bucket,
		        sumMerge(hist_count)                                      AS hc,
		        sumMerge(hist_sum)                                        AS hs,
		        hs / nullIf(hc, 0)                                        AS avg_sec
		    FROM %s
		    WHERE team_id = @teamID
		      AND bucket_ts BETWEEN @start AND @end
		      AND metric_name = @metricName
		      %s
		    GROUP BY time_bucket, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address
		    HAVING hc > 0
		)
		GROUP BY time_bucket, bucket_label
		ORDER BY time_bucket, bucket_label
	`, shared.BucketTimeExpr, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)

	var dtos []latencyHeatmapDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "latency.GetLatencyHeatmap", &dtos, query, args...); err != nil {
		return nil, err
	}

	counts := map[string]int64{}
	for _, d := range dtos {
		counts[d.TimeBucket] += d.Count
	}

	out := make([]LatencyHeatmapBucket, len(dtos))
	for i, d := range dtos {
		density := 0.0
		if total := counts[d.TimeBucket]; total > 0 {
			density = float64(d.Count) / float64(total)
		}
		out[i] = LatencyHeatmapBucket{
			TimeBucket:	d.TimeBucket,
			BucketLabel:	d.BucketLabel,
			Count:		d.Count,
			Density:	density,
		}
	}
	return out, nil
}
