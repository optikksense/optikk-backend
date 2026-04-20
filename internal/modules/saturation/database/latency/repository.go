package latency

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
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

type latencyDTO struct {
	TimeBucket   string   `ch:"time_bucket"`
	GroupBy      string   `ch:"group_by"`
	LatencySum   float64  `ch:"latency_sum"`
	LatencyCount int64    `ch:"latency_count"`
	P50Ms        *float64 `ch:"p50_ms"`
	P95Ms        *float64 `ch:"p95_ms"`
	P99Ms        *float64 `ch:"p99_ms"`
}

// latencySeriesByAttr returns raw sum/count per (time_bucket, group_by).
// Percentile columns (p50/p95/p99) are emitted as 0 placeholders so existing
// scan shapes hold; service fills them from the DbOpLatency sketch where the
// dim-prefix supports it (currently only by-system — see service.go).
func (r *ClickHouseRepository) latencySeriesByAttr(ctx context.Context, teamID int64, startMs, endMs int64, groupAttr string, f shared.Filters) ([]LatencyTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                        AS time_bucket,
		    %s                        AS group_by,
		    sum(hist_sum)             AS latency_sum,
		    toInt64(sum(hist_count))  AS latency_count,
		    CAST(0 AS Nullable(Float64)) AS p50_ms,
		    CAST(0 AS Nullable(Float64)) AS p95_ms,
		    CAST(0 AS Nullable(Float64)) AS p99_ms
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, shared.AttrString(groupAttr),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)

	var dtos []latencyDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}

	rows := make([]LatencyTimeSeries, len(dtos))
	for i, d := range dtos {
		rows[i] = LatencyTimeSeries{
			TimeBucket:   d.TimeBucket,
			GroupBy:      d.GroupBy,
			P50Ms:        d.P50Ms,
			P95Ms:        d.P95Ms,
			P99Ms:        d.P99Ms,
			LatencySum:   d.LatencySum,
			LatencyCount: d.LatencyCount,
		}
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

func (r *ClickHouseRepository) GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyHeatmapBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                    AS time_bucket,
		    multiIf(
		        hist_sum / nullIf(hist_count, 0) < 0.001,  '< 1ms',
		        hist_sum / nullIf(hist_count, 0) < 0.005,  '1–5ms',
		        hist_sum / nullIf(hist_count, 0) < 0.010,  '5–10ms',
		        hist_sum / nullIf(hist_count, 0) < 0.025,  '10–25ms',
		        hist_sum / nullIf(hist_count, 0) < 0.050,  '25–50ms',
		        hist_sum / nullIf(hist_count, 0) < 0.100,  '50–100ms',
		        hist_sum / nullIf(hist_count, 0) < 0.250,  '100–250ms',
		        hist_sum / nullIf(hist_count, 0) < 0.500,  '250–500ms',
		        hist_sum / nullIf(hist_count, 0) < 1.000,  '500ms–1s',
		        '> 1s'
		    )                                                                     AS bucket_label,
		    toInt64(sum(hist_count))                                              AS count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND hist_count > 0
		  %s
		GROUP BY time_bucket, bucket_label
		ORDER BY time_bucket, bucket_label
	`,
		bucket,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)

	var dtos []latencyHeatmapDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
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
			TimeBucket:  d.TimeBucket,
			BucketLabel: d.BucketLabel,
			Count:       d.Count,
			Density:     density,
		}
	}
	return out, nil
}
