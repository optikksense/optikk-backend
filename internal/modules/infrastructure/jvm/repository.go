package jvm

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	GetJVMMemory(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmMemoryBucketDTO, error)
	GetJVMGCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetJVMGCCollections(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (jvmClassStatsDTO, error)
	GetJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) (jvmCPUStatsDTO, error)
	GetJVMBuffers(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmBufferBucketDTO, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func bucketExpr(startMs, endMs int64) string {
	return infraconsts.TimeBucketExpression(startMs, endMs)
}

func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
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
	err := r.db.Select(ctx, &rows, query, baseParams(teamID, startMs, endMs)...)
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

func (r *ClickHouseRepository) GetJVMGCCollections(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	bucket := bucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as pod, sum(hist_count) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s = '%s' AND metric_type = 'Histogram'
		GROUP BY 1 ORDER BY 1`,
		bucket,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMGCDuration)
	return r.queryResourceBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	bucket := bucketExpr(startMs, endMs)
	daemon := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMThreadDaemon)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, daemon, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp, infraconsts.ColMetricName, infraconsts.MetricJVMThreadCount)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (jvmClassStatsDTO, error) {
	query := fmt.Sprintf(`
		SELECT
			sumIf(%s, %s = '%s') as loaded,
			avgIf(%s, %s = '%s') as count_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')`,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMClassLoaded,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMClassCount,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMClassLoaded, infraconsts.MetricJVMClassCount)
	var row jvmClassStatsDTO
	err := r.db.QueryRow(ctx, &row, query, baseParams(teamID, startMs, endMs)...)
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
	err := r.db.QueryRow(ctx, &row, query, baseParams(teamID, startMs, endMs)...)
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
	err := r.db.Select(ctx, &rows, query, baseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) queryStateBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	var rows []StateBucket
	err := r.db.Select(ctx, &rows, query, baseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) queryResourceBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	var rows []ResourceBucket
	err := r.db.Select(ctx, &rows, query, baseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) queryHistogramSummary(ctx context.Context, query string, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	var row HistogramSummary
	err := r.db.QueryRow(ctx, &row, query, baseParams(teamID, startMs, endMs)...)
	return row, err
}
