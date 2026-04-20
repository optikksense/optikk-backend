package jvm

import (
	"context"
	"fmt"
	"math"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	GetJVMMemory(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmMemoryBucketRow, error)
	GetJVMGCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetJVMGCCollections(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmGCCollectionBucketDTO, error)
	GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmThreadBucketRow, error)
	GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (jvmClassStatsRow, error)
	GetJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) (jvmCPUStatsRow, error)
	GetJVMBuffers(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmBufferBucketRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func bucketExpr(startMs, endMs int64) string {
	return infraconsts.TimeBucketExpression(startMs, endMs)
}

// histogramSummaryRow carries the sum/count pair for histogram-backed metrics.
// Percentiles come from sketch.Querier (JvmMetricLatency).
type histogramSummaryRow struct {
	HistSum   float64 `ch:"hist_sum"`
	HistCount int64   `ch:"hist_count"`
	P50       float64 `ch:"p50"`
	P95       float64 `ch:"p95"`
	P99       float64 `ch:"p99"`
}

// jvmMemoryBucketRow carries sum/count per memory kind so the service can
// compute avg in Go (replaces three avgIf() calls in SQL).
type jvmMemoryBucketRow struct {
	Timestamp      string  `ch:"time_bucket"`
	PoolName       string  `ch:"pool_name"`
	MemType        string  `ch:"mem_type"`
	UsedSum        float64 `ch:"used_sum"`
	UsedCount      int64   `ch:"used_count"`
	CommittedSum   float64 `ch:"committed_sum"`
	CommittedCount int64   `ch:"committed_count"`
	LimitSum       float64 `ch:"limit_sum"`
	LimitCount     int64   `ch:"limit_count"`
}

// jvmThreadBucketRow carries sum/count per (bucket, daemon).
type jvmThreadBucketRow struct {
	Timestamp string  `ch:"time_bucket"`
	Daemon    string  `ch:"daemon"`
	ValSum    float64 `ch:"val_sum"`
	ValCount  int64   `ch:"val_count"`
}

// jvmClassStatsRow carries totals; the service derives the final shape.
type jvmClassStatsRow struct {
	LoadedSum float64 `ch:"loaded_sum"`
	CountSum  float64 `ch:"count_sum"`
	CountN    int64   `ch:"count_n"`
}

// jvmCPUStatsRow — cpu_time is a running total (sumIf), cpu_util is an avg.
type jvmCPUStatsRow struct {
	CPUTimeSum  float64 `ch:"cpu_time_sum"`
	CPUUtilSum  float64 `ch:"cpu_util_sum"`
	CPUUtilN    int64   `ch:"cpu_util_n"`
}

// jvmBufferBucketRow carries sum/count pairs for memory_usage and count.
type jvmBufferBucketRow struct {
	Timestamp        string  `ch:"time_bucket"`
	PoolName         string  `ch:"pool_name"`
	MemoryUsageSum   float64 `ch:"memory_usage_sum"`
	MemoryUsageCount int64   `ch:"memory_usage_count"`
	BufCountSum      float64 `ch:"buf_count_sum"`
	BufCountN        int64   `ch:"buf_count_n"`
}

func (r *ClickHouseRepository) GetJVMMemory(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmMemoryBucketRow, error) {
	bucket := bucketExpr(startMs, endMs)
	pool := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMMemoryPoolName)
	memType := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMMemoryType)
	query := fmt.Sprintf(`
		SELECT
			%s as time_bucket,
			%s as pool_name,
			%s as mem_type,
			sumIf(%s, %s = '%s')          as used_sum,
			toInt64(countIf(%s = '%s'))   as used_count,
			sumIf(%s, %s = '%s')          as committed_sum,
			toInt64(countIf(%s = '%s'))   as committed_count,
			sumIf(%s, %s = '%s')          as limit_sum,
			toInt64(countIf(%s = '%s'))   as limit_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s', '%s')
		GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`,
		bucket, pool, memType,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed,
		infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryCommitted,
		infraconsts.ColMetricName, infraconsts.MetricJVMMemoryCommitted,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryLimit,
		infraconsts.ColMetricName, infraconsts.MetricJVMMemoryLimit,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed, infraconsts.MetricJVMMemoryCommitted, infraconsts.MetricJVMMemoryLimit)
	var rows []jvmMemoryBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetJVMGCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error) {
	// Quantiles + avg both come from hist_sum / hist_count roll-ups via
	// sketch.JvmMetricLatency. SQL only fetches the raw totals.
	query := fmt.Sprintf(`
		SELECT
			sum(hist_sum)            AS hist_sum,
			toInt64(sum(hist_count)) AS hist_count,
			0 AS p50, 0 AS p95, 0 AS p99
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s = '%s' AND metric_type = 'Histogram'`,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMGCDuration)
	var row histogramSummaryRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...).ScanStruct(&row); err != nil {
		return histogramSummaryRow{}, err
	}
	return row, nil
}

func (r *ClickHouseRepository) GetJVMGCCollections(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmGCCollectionBucketDTO, error) {
	bucket := bucketExpr(startMs, endMs)
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

func (r *ClickHouseRepository) GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmThreadBucketRow, error) {
	bucket := bucketExpr(startMs, endMs)
	daemon := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMThreadDaemon)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as daemon,
		       sum(%s)          as val_sum,
		       toInt64(count()) as val_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, daemon, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp, infraconsts.ColMetricName, infraconsts.MetricJVMThreadCount)
	var rows []jvmThreadBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (jvmClassStatsRow, error) {
	query := fmt.Sprintf(`
		SELECT
			sumIf(%s, %s = '%s' AND isFinite(%s))                 AS loaded_sum,
			sumIf(%s, %s = '%s' AND isFinite(%s))                 AS count_sum,
			toInt64(countIf(%s = '%s' AND isFinite(%s)))          AS count_n
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')`,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMClassLoaded, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMClassCount, infraconsts.ColValue,
		infraconsts.ColMetricName, infraconsts.MetricJVMClassCount, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMClassLoaded, infraconsts.MetricJVMClassCount)
	var row jvmClassStatsRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...).ScanStruct(&row)
	return row, err
}

func (r *ClickHouseRepository) GetJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) (jvmCPUStatsRow, error) {
	query := fmt.Sprintf(`
		SELECT
			sumIf(%s, %s = '%s')                        AS cpu_time_sum,
			sumIf(%s, %s = '%s')                        AS cpu_util_sum,
			toInt64(countIf(%s = '%s'))                 AS cpu_util_n
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')`,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMCPUTime,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMCPUUtilization,
		infraconsts.ColMetricName, infraconsts.MetricJVMCPUUtilization,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMCPUTime, infraconsts.MetricJVMCPUUtilization)
	var row jvmCPUStatsRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...).ScanStruct(&row)
	return row, err
}

func (r *ClickHouseRepository) GetJVMBuffers(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmBufferBucketRow, error) {
	bucket := bucketExpr(startMs, endMs)
	pool := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMBufferPoolName)
	query := fmt.Sprintf(`
		SELECT
			%s as time_bucket,
			%s as pool_name,
			sumIf(%s, %s = '%s')          as memory_usage_sum,
			toInt64(countIf(%s = '%s'))   as memory_usage_count,
			sumIf(%s, %s = '%s')          as buf_count_sum,
			toInt64(countIf(%s = '%s'))   as buf_count_n
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, pool,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMBufferMemoryUsage,
		infraconsts.ColMetricName, infraconsts.MetricJVMBufferMemoryUsage,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMBufferCount,
		infraconsts.ColMetricName, infraconsts.MetricJVMBufferCount,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMBufferMemoryUsage, infraconsts.MetricJVMBufferCount)
	var rows []jvmBufferBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
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
