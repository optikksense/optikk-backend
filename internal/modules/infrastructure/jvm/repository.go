package jvm

import (
	"context"
	"fmt"
	"math"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
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
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func bucketExpr(startMs, endMs int64) string {
	return infraconsts.TimeBucketExpression(startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMMemory(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmMemoryBucketDTO, error) {
	bucket := bucketExpr(startMs, endMs)
	pool := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMMemoryPoolName)
	memType := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMMemoryType)
	query := fmt.Sprintf(`
		SELECT
			%s as time_bucket,
			%s as pool_name,
			%s as mem_type,
			avgIf(%s, %s = '%s') as used,
			avgIf(%s, %s = '%s') as committed,
			avgIf(%s, %s = '%s') as limit_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s', '%s')
		GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`,
		bucket, pool, memType,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryCommitted,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryLimit,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed, infraconsts.MetricJVMMemoryCommitted, infraconsts.MetricJVMMemoryLimit)
	var rows []jvmMemoryBucketDTO
	err := r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	for i := range rows {
		rows[i].Used = sanitizeFloatPtr(rows[i].Used)
		rows[i].Committed = sanitizeFloatPtr(rows[i].Committed)
		rows[i].Limit = sanitizeFloatPtr(rows[i].Limit)
	}
	return rows, err
}

func (r *ClickHouseRepository) GetJVMGCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error) {
	query := fmt.Sprintf(`
		SELECT
			quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) as p50,
			quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) as p95,
			quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) as p99,
			avg(hist_sum / nullIf(hist_count, 0)) as avg_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s = '%s' AND metric_type = 'Histogram'`,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMGCDuration)
	return r.queryHistogramSummary(ctx, query, teamID, startMs, endMs)
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
	return r.queryJVMGCCollectionBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmThreadBucketDTO, error) {
	bucket := bucketExpr(startMs, endMs)
	daemon := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMThreadDaemon)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as daemon, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, daemon, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp, infraconsts.ColMetricName, infraconsts.MetricJVMThreadCount)
	return r.queryJVMThreadBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (jvmClassStatsDTO, error) {
	loadedExpr := fmt.Sprintf("sumIf(%s, %s = '%s' AND isFinite(%s))", infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMClassLoaded, infraconsts.ColValue)
	countExpr := fmt.Sprintf("avgIf(%s, %s = '%s' AND isFinite(%s))", infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMClassCount, infraconsts.ColValue)
	query := fmt.Sprintf(`
		SELECT
			toInt64(if(isFinite(%s), round(%s), 0)) as loaded,
			toInt64(if(isFinite(%s), round(%s), 0)) as count_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')`,
		loadedExpr, loadedExpr,
		countExpr, countExpr,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMClassLoaded, infraconsts.MetricJVMClassCount)
	var row jvmClassStatsDTO
	err := r.db.QueryRow(ctx, &row, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	return row, err
}

func (r *ClickHouseRepository) GetJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) (jvmCPUStatsDTO, error) {
	query := fmt.Sprintf(`
		SELECT
			sumIf(%s, %s = '%s') as cpu_time,
			avgIf(%s, %s = '%s') as cpu_util
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')`,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMCPUTime,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMCPUUtilization,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMCPUTime, infraconsts.MetricJVMCPUUtilization)
	var row jvmCPUStatsDTO
	err := r.db.QueryRow(ctx, &row, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	row.CPUTimeValue = sanitizeFloat(row.CPUTimeValue)
	row.RecentUtilization = sanitizeFloat(row.RecentUtilization)
	return row, err
}

func (r *ClickHouseRepository) GetJVMBuffers(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmBufferBucketDTO, error) {
	bucket := bucketExpr(startMs, endMs)
	pool := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMBufferPoolName)
	query := fmt.Sprintf(`
		SELECT
			%s as time_bucket,
			%s as pool_name,
			avgIf(%s, %s = '%s') as memory_usage,
			avgIf(%s, %s = '%s') as buf_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, pool,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMBufferMemoryUsage,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMBufferCount,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMBufferMemoryUsage, infraconsts.MetricJVMBufferCount)
	var rows []JVMBufferBucket
	err := r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	for i := range rows {
		rows[i].MemoryUsage = sanitizeFloatPtr(rows[i].MemoryUsage)
		rows[i].Count = sanitizeFloatPtr(rows[i].Count)
	}
	return rows, err
}

func (r *ClickHouseRepository) queryJVMThreadBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]JVMThreadBucket, error) {
	var rows []JVMThreadBucket
	err := r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	for i := range rows {
		rows[i].Value = sanitizeFloatPtr(rows[i].Value)
	}
	return rows, err
}

func (r *ClickHouseRepository) queryJVMGCCollectionBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]JVMGCCollectionBucket, error) {
	var rows []JVMGCCollectionBucket
	err := r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	for i := range rows {
		rows[i].Value = sanitizeFloatPtr(rows[i].Value)
	}
	return rows, err
}

func (r *ClickHouseRepository) queryHistogramSummary(ctx context.Context, query string, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	var row HistogramSummary
	err := r.db.QueryRow(ctx, &row, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	row.P50 = sanitizeFloat(row.P50)
	row.P95 = sanitizeFloat(row.P95)
	row.P99 = sanitizeFloat(row.P99)
	row.Avg = sanitizeFloat(row.Avg)
	return row, err
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
