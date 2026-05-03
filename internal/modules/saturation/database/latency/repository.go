package latency

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

// Repository runs latency-by-* panels against `observability.spans_1m`.
// Each method computes p50/p95/p99 server-side via quantileTimingMerge on
// the rollup's latency_state; service.go is a trivial passthrough. The
// heatmap path stays on raw spans (needs per-span band classification).
type Repository interface {
	GetLatencyBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error)
	GetLatencyByOperation(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error)
	GetLatencyByCollection(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error)
	GetLatencyByNamespace(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error)
	GetLatencyByServer(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error)
	GetLatencyHeatmap(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyHeatmapRawDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type latencyRawDTO struct {
	TimeBucket string  `ch:"time_bucket"`
	GroupBy    string  `ch:"group_by"`
	P50Ms      float64 `ch:"p50_ms"`
	P95Ms      float64 `ch:"p95_ms"`
	P99Ms      float64 `ch:"p99_ms"`
}

type latencyHeatmapRawDTO struct {
	TimeBucket  string `ch:"time_bucket"`
	BucketLabel string `ch:"bucket_label"`
	Count       int64  `ch:"count"`
}

func (r *ClickHouseRepository) GetLatencyBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error) {
	return r.latencySeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBSystem, "latency.GetLatencyBySystem")
}

func (r *ClickHouseRepository) GetLatencyByOperation(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error) {
	return r.latencySeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBOperationName, "latency.GetLatencyByOperation")
}

func (r *ClickHouseRepository) GetLatencyByCollection(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error) {
	return r.latencySeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBCollectionName, "latency.GetLatencyByCollection")
}

func (r *ClickHouseRepository) GetLatencyByNamespace(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error) {
	return r.latencySeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBNamespace, "latency.GetLatencyByNamespace")
}

func (r *ClickHouseRepository) GetLatencyByServer(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error) {
	return r.latencySeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrServerAddress, "latency.GetLatencyByServer")
}

func (r *ClickHouseRepository) latencySeriesByGroup(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, attr, traceLabel string) ([]latencyRawDTO, error) {
	groupCol := filter.Spans1mGroupColumn(attr)
	if groupCol == "" {
		return nil, nil
	}
	filterWhere, filterArgs := filter.BuildSpans1mClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(toDateTime(ts_bucket))                  AS time_bucket,
		       ` + groupCol + `                                  AS group_by,
		       quantileTimingMerge(0.5)(latency_state)           AS p50_ms,
		       quantileTimingMerge(0.95)(latency_state)          AS p95_ms,
		       quantileTimingMerge(0.99)(latency_state)          AS p99_ms
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''` + filterWhere + `
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	var rows []latencyRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, traceLabel, &rows, query, args...)
}

// GetLatencyHeatmap classifies each span into a coarse latency band based
// on its raw duration_nano, then counts per (time_bucket, band). No avg/
// digest tricks — the raw column is right there.
func (r *ClickHouseRepository) GetLatencyHeatmap(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyHeatmapRawDTO, error) {
	filterWhere, filterArgs := filter.BuildSpanClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(toDateTime(ts_bucket)) AS time_bucket,
		       multiIf(
		           duration_nano <    1000000, '< 1ms',
		           duration_nano <    5000000, '1-5ms',
		           duration_nano <   10000000, '5-10ms',
		           duration_nano <   25000000, '10-25ms',
		           duration_nano <   50000000, '25-50ms',
		           duration_nano <  100000000, '50-100ms',
		           duration_nano <  250000000, '100-250ms',
		           duration_nano <  500000000, '250-500ms',
		           duration_nano < 1000000000, '500ms-1s',
		           '> 1s'
		       )                              AS bucket_label,
		       toInt64(count())               AS count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''` + filterWhere + `
		GROUP BY time_bucket, bucket_label
		ORDER BY time_bucket, bucket_label`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	var rows []latencyHeatmapRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "latency.GetLatencyHeatmap", &rows, query, args...)
}
