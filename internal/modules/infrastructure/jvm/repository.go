package jvm

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

// JVM metrics read from the Phase-9 cascade:
//
//   - gauge metrics (jvm.memory.*, jvm.thread.count, jvm.classes.*, jvm.cpu.*,
//     jvm.buffer.*) → metrics_gauges_rollup via extended state_dim
//     extractor (pool.name | type, thread.daemon, buffer.pool.name).
//   - histogram metrics (jvm.gc.duration) → `metrics_histograms_rollup`.
//
// GetJVMGCCollections stays on raw because grouping by `jvm.gc.name` isn't a
// rollup dim (the histograms rollup keys only metric_name + service). Future
// work: extend metrics_histograms_rollup with a generic-attribute dim column.
const (
	metricsGaugesRollupPrefix = "observability.metrics_gauges_rollup"
	metricsHistPrefix     = "observability.metrics_histograms_rollup"
)

type Repository interface {
	GetJVMMemory(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmMemoryBucketDTO, error)
	GetJVMGCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetJVMGCCollections(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmGCCollectionBucketDTO, error)
	GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmThreadBucketDTO, error)
	GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (jvmClassStatsDTO, error)
	GetJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) (jvmCPUStatsDTO, error)
	GetJVMBuffers(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmBufferBucketDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func bucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumn(startMs, endMs, "bucket_ts")
}

// GetJVMMemory groups by (pool_name, mem_type) from the combined `state_dim`
// column (MV emits `concat(pool, '|', type)`). Values split across 3
// metric_names; folded client-side.
func (r *ClickHouseRepository) GetJVMMemory(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmMemoryBucketDTO, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                         AS time_bucket,
		    state_dim                                                                  AS state_dim,
		    metric_name                                                                AS metric_name,
		    sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)     AS val
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN (@used, @committed, @limit)
		GROUP BY time_bucket, state_dim, metric_name
		ORDER BY time_bucket ASC, state_dim ASC
	`, bucketExpr(startMs, endMs), table)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("used", infraconsts.MetricJVMMemoryUsed),
		clickhouse.Named("committed", infraconsts.MetricJVMMemoryCommitted),
		clickhouse.Named("limit", infraconsts.MetricJVMMemoryLimit),
	)
	var metricRows []struct {
		TimeBucket string  `ch:"time_bucket"`
		StateDim   string  `ch:"state_dim"`
		MetricName string  `ch:"metric_name"`
		Val        float64 `ch:"val"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &metricRows, query, args...); err != nil {
		return nil, err
	}
	// Fold per (time_bucket, pool_name, mem_type).
	type key struct{ bucket, pool, memType string }
	out := map[key]*jvmMemoryBucketDTO{}
	for _, mr := range metricRows {
		pool, memType, _ := strings.Cut(mr.StateDim, "|")
		k := key{mr.TimeBucket, pool, memType}
		row, ok := out[k]
		if !ok {
			row = &jvmMemoryBucketDTO{Timestamp: k.bucket, PoolName: k.pool, MemType: k.memType}
			out[k] = row
		}
		v := mr.Val
		switch mr.MetricName {
		case infraconsts.MetricJVMMemoryUsed:
			row.Used = sanitizeFloatPtr(&v)
		case infraconsts.MetricJVMMemoryCommitted:
			row.Committed = sanitizeFloatPtr(&v)
		case infraconsts.MetricJVMMemoryLimit:
			row.Limit = sanitizeFloatPtr(&v)
		}
	}
	rows := make([]jvmMemoryBucketDTO, 0, len(out))
	for _, row := range out {
		rows = append(rows, *row)
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetJVMGCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error) {
	table, _ := rollup.TierTableFor(metricsHistPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1])  AS p50,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])  AS p95,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3])  AS p99,
		    sumMerge(hist_sum) / nullIf(toFloat64(sumMerge(hist_count)), 0)      AS avg_val
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName`, table)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("metricName", infraconsts.MetricJVMGCDuration),
	)
	var row HistogramSummary
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return row, err
	}
	row.P50 = sanitizeFloat(row.P50)
	row.P95 = sanitizeFloat(row.P95)
	row.P99 = sanitizeFloat(row.P99)
	row.Avg = sanitizeFloat(row.Avg)
	return row, nil
}

// GetJVMGCCollections groups by `jvm.gc.name` — a dim the histograms rollup
// doesn't key. Stays on raw metrics. Future: extend rollup with a generic
// attribute dim or per-metric-family dedicated rollup.
func (r *ClickHouseRepository) GetJVMGCCollections(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmGCCollectionBucketDTO, error) {
	bucket := infraconsts.TimeBucketExpression(startMs, endMs)
	collector := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMGCName)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as collector, toFloat64(sum(hist_count)) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s = '%s' AND metric_type = 'Histogram'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, collector,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMGCDuration)
	var rows []JVMGCCollectionBucket
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	for i := range rows {
		rows[i].Value = sanitizeFloatPtr(rows[i].Value)
	}
	return rows, err
}

func (r *ClickHouseRepository) GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmThreadBucketDTO, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                         AS time_bucket,
		    state_dim                                                                  AS daemon,
		    sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)     AS metric_val
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		GROUP BY time_bucket, daemon
		ORDER BY time_bucket, daemon`, bucketExpr(startMs, endMs), table)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("metricName", infraconsts.MetricJVMThreadCount),
	)
	var rows []JVMThreadBucket
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...)
	for i := range rows {
		rows[i].Value = sanitizeFloatPtr(rows[i].Value)
	}
	return rows, err
}

func (r *ClickHouseRepository) GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (jvmClassStatsDTO, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT metric_name                                                             AS metric_name,
		       sumMerge(value_sum)                                                     AS val_sum,
		       sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)  AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN (@loaded, @countMetric)
		GROUP BY metric_name`, table)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("loaded", infraconsts.MetricJVMClassLoaded),
		clickhouse.Named("countMetric", infraconsts.MetricJVMClassCount),
	)
	var metricRows []struct {
		MetricName string  `ch:"metric_name"`
		ValSum     float64 `ch:"val_sum"`
		ValAvg     float64 `ch:"val_avg"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &metricRows, query, args...); err != nil {
		return jvmClassStatsDTO{}, err
	}
	var row jvmClassStatsDTO
	for _, mr := range metricRows {
		switch mr.MetricName {
		case infraconsts.MetricJVMClassLoaded:
			if !math.IsNaN(mr.ValSum) && !math.IsInf(mr.ValSum, 0) {
				row.Loaded = int64(math.Round(mr.ValSum))
			}
		case infraconsts.MetricJVMClassCount:
			if !math.IsNaN(mr.ValAvg) && !math.IsInf(mr.ValAvg, 0) {
				row.Count = int64(math.Round(mr.ValAvg))
			}
		}
	}
	return row, nil
}

func (r *ClickHouseRepository) GetJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) (jvmCPUStatsDTO, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT metric_name                                                             AS metric_name,
		       sumMerge(value_sum)                                                     AS val_sum,
		       sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)  AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN (@time, @util)
		GROUP BY metric_name`, table)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("time", infraconsts.MetricJVMCPUTime),
		clickhouse.Named("util", infraconsts.MetricJVMCPUUtilization),
	)
	var metricRows []struct {
		MetricName string  `ch:"metric_name"`
		ValSum     float64 `ch:"val_sum"`
		ValAvg     float64 `ch:"val_avg"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &metricRows, query, args...); err != nil {
		return jvmCPUStatsDTO{}, err
	}
	var row jvmCPUStatsDTO
	for _, mr := range metricRows {
		switch mr.MetricName {
		case infraconsts.MetricJVMCPUTime:
			row.CPUTimeValue = sanitizeFloat(mr.ValSum)
		case infraconsts.MetricJVMCPUUtilization:
			row.RecentUtilization = sanitizeFloat(mr.ValAvg)
		}
	}
	return row, nil
}

func (r *ClickHouseRepository) GetJVMBuffers(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmBufferBucketDTO, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                         AS time_bucket,
		    state_dim                                                                  AS pool_name,
		    metric_name                                                                AS metric_name,
		    sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)     AS val
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN (@mem, @cnt)
		GROUP BY time_bucket, pool_name, metric_name
		ORDER BY time_bucket, pool_name`, bucketExpr(startMs, endMs), table)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("mem", infraconsts.MetricJVMBufferMemoryUsage),
		clickhouse.Named("cnt", infraconsts.MetricJVMBufferCount),
	)
	var metricRows []struct {
		TimeBucket string  `ch:"time_bucket"`
		PoolName   string  `ch:"pool_name"`
		MetricName string  `ch:"metric_name"`
		Val        float64 `ch:"val"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &metricRows, query, args...); err != nil {
		return nil, err
	}
	type key struct{ bucket, pool string }
	out := map[key]*JVMBufferBucket{}
	for _, mr := range metricRows {
		k := key{mr.TimeBucket, mr.PoolName}
		row, ok := out[k]
		if !ok {
			row = &JVMBufferBucket{Timestamp: k.bucket, PoolName: k.pool}
			out[k] = row
		}
		v := mr.Val
		switch mr.MetricName {
		case infraconsts.MetricJVMBufferMemoryUsage:
			row.MemoryUsage = sanitizeFloatPtr(&v)
		case infraconsts.MetricJVMBufferCount:
			row.Count = sanitizeFloatPtr(&v)
		}
	}
	rows := make([]JVMBufferBucket, 0, len(out))
	for _, row := range out {
		rows = append(rows, *row)
	}
	return rows, nil
}

func sanitizeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

func sanitizeFloatPtr(v *float64) *float64 {
	if v == nil {
		return nil
	}
	if math.IsNaN(*v) || math.IsInf(*v, 0) {
		return nil
	}
	return v
}
