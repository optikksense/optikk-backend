package jvm

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	GetJVMMemory(teamID int64, startMs, endMs int64) ([]JVMMemoryBucket, error)
	GetJVMGCDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetJVMGCCollections(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetJVMThreadCount(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetJVMClasses(teamID int64, startMs, endMs int64) (JVMClassStats, error)
	GetJVMCPU(teamID int64, startMs, endMs int64) (JVMCPUStats, error)
	GetJVMBuffers(teamID int64, startMs, endMs int64) ([]JVMBufferBucket, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func bucketExpr(startMs, endMs int64) string {
	return infraconsts.TimeBucketExpression(startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMMemory(teamID int64, startMs, endMs int64) ([]JVMMemoryBucket, error) {
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
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s', '%s')
		GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`,
		bucket, pool, memType,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryCommitted,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryLimit,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed, infraconsts.MetricJVMMemoryCommitted, infraconsts.MetricJVMMemoryLimit)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]JVMMemoryBucket, len(rows))
	for i, row := range rows {
		buckets[i] = JVMMemoryBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			PoolName:  dbutil.StringFromAny(row["pool_name"]),
			MemType:   dbutil.StringFromAny(row["mem_type"]),
			Used:      dbutil.NullableFloat64FromAny(row["used"]),
			Committed: dbutil.NullableFloat64FromAny(row["committed"]),
			Limit:     dbutil.NullableFloat64FromAny(row["limit_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetJVMGCDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	query := fmt.Sprintf(`
		SELECT
			quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) as p50,
			quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) as p95,
			quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) as p99,
			avg(hist_sum / nullIf(hist_count, 0)) as avg_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s = '%s' AND metric_type = 'Histogram'`,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMGCDuration)
	return r.queryHistogramSummary(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMGCCollections(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	bucket := bucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as pod, sum(hist_count) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s = '%s' AND metric_type = 'Histogram'
		GROUP BY 1 ORDER BY 1`,
		bucket,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMGCDuration)
	return r.queryResourceBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMThreadCount(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	bucket := bucketExpr(startMs, endMs)
	daemon := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMThreadDaemon)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, daemon, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp, infraconsts.ColMetricName, infraconsts.MetricJVMThreadCount)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMClasses(teamID int64, startMs, endMs int64) (JVMClassStats, error) {
	query := fmt.Sprintf(`
		SELECT
			sumIf(%s, %s = '%s') as loaded,
			avgIf(%s, %s = '%s') as count_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')`,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMClassLoaded,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMClassCount,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMClassLoaded, infraconsts.MetricJVMClassCount)
	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return JVMClassStats{}, err
	}
	return JVMClassStats{
		Loaded: dbutil.Int64FromAny(row["loaded"]),
		Count:  dbutil.Int64FromAny(row["count_val"]),
	}, nil
}

func (r *ClickHouseRepository) GetJVMCPU(teamID int64, startMs, endMs int64) (JVMCPUStats, error) {
	query := fmt.Sprintf(`
		SELECT
			sumIf(%s, %s = '%s') as cpu_time,
			avgIf(%s, %s = '%s') as cpu_util
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')`,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMCPUTime,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMCPUUtilization,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMCPUTime, infraconsts.MetricJVMCPUUtilization)
	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return JVMCPUStats{}, err
	}
	return JVMCPUStats{
		CPUTimeValue:      dbutil.Float64FromAny(row["cpu_time"]),
		RecentUtilization: dbutil.Float64FromAny(row["cpu_util"]),
	}, nil
}

func (r *ClickHouseRepository) GetJVMBuffers(teamID int64, startMs, endMs int64) ([]JVMBufferBucket, error) {
	bucket := bucketExpr(startMs, endMs)
	pool := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMBufferPoolName)
	query := fmt.Sprintf(`
		SELECT
			%s as time_bucket,
			%s as pool_name,
			avgIf(%s, %s = '%s') as memory_usage,
			avgIf(%s, %s = '%s') as buf_count
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, pool,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMBufferMemoryUsage,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMBufferCount,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMBufferMemoryUsage, infraconsts.MetricJVMBufferCount)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]JVMBufferBucket, len(rows))
	for i, row := range rows {
		buckets[i] = JVMBufferBucket{
			Timestamp:   dbutil.StringFromAny(row["time_bucket"]),
			PoolName:    dbutil.StringFromAny(row["pool_name"]),
			MemoryUsage: dbutil.NullableFloat64FromAny(row["memory_usage"]),
			Count:       dbutil.NullableFloat64FromAny(row["buf_count"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) queryStateBuckets(query string, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]StateBucket, len(rows))
	for i, row := range rows {
		buckets[i] = StateBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			State:     dbutil.StringFromAny(row["state"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) queryResourceBuckets(query string, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]ResourceBucket, len(rows))
	for i, row := range rows {
		buckets[i] = ResourceBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Pod:       dbutil.StringFromAny(row["pod"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) queryHistogramSummary(query string, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return HistogramSummary{}, err
	}
	return HistogramSummary{
		P50: dbutil.Float64FromAny(row["p50"]),
		P95: dbutil.Float64FromAny(row["p95"]),
		P99: dbutil.Float64FromAny(row["p99"]),
		Avg: dbutil.Float64FromAny(row["avg_val"]),
	}, nil
}
