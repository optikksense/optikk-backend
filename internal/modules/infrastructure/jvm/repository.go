package jvm

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	QueryJVMMemoryByPool(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMMemoryRow, error)
	QueryJVMGCDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (JVMHistogramAggRow, error)
	QueryJVMGCCollectionsByName(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMGCRow, error)
	QueryJVMThreadCountByDaemon(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMThreadRow, error)
	QueryJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMMetricNameRow, error)
	QueryJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMMetricNameRow, error)
	QueryJVMBuffersByPool(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMBufferRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

// QueryJVMMemoryByPool returns per-(timestamp, pool, mem_type, metric_name)
// average values for the 3-metric JVM memory family.
func (r *ClickHouseRepository) QueryJVMMemoryByPool(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMMemoryRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'jvm.memory.pool.name'::String                AS pool_name,
		    attributes.'jvm.memory.type'::String                     AS mem_type,
		    metric_name                                              AS metric_name,
		    value                                                    AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'jvm.memory.pool.name'::String != ''
		ORDER BY timestamp`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), []string{
		infraconsts.MetricJVMMemoryUsed,
		infraconsts.MetricJVMMemoryCommitted,
		infraconsts.MetricJVMMemoryLimit,
	})
	var rows []JVMMemoryRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "jvm.QueryJVMMemoryByPool", &rows, query, args...)
}

// QueryJVMGCDurationHistogram returns the merged histogram across the window
// for jvm.gc.duration. Service computes P50/P95/P99 via quantile.FromHistogram
// and Avg = sum_hist_sum / sum_hist_count.
func (r *ClickHouseRepository) QueryJVMGCDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (JVMHistogramAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    sum(hist_sum)            AS sum_hist_sum,
		    sum(hist_count)          AS sum_hist_count,
		    any(hist_buckets)        AS buckets,
		    sumForEach(hist_counts)  AS counts
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end`
	args := withMetricName(metricArgs(teamID, startMs, endMs), infraconsts.MetricJVMGCDuration)
	var row JVMHistogramAggRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "jvm.QueryJVMGCDurationHistogram", &row, query, args...)
}

// QueryJVMGCCollectionsByName reads `hist_count` (count side of the same
// histogram) per (timestamp, gc_name) for the per-collector collection panel.
func (r *ClickHouseRepository) QueryJVMGCCollectionsByName(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMGCRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'jvm.gc.name'::String                         AS gc_name,
		    toFloat64(hist_count)                                    AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'jvm.gc.name'::String != ''
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), infraconsts.MetricJVMGCDuration)
	var rows []JVMGCRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "jvm.QueryJVMGCCollectionsByName", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryJVMThreadCountByDaemon(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMThreadRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'jvm.thread.daemon'::String                   AS daemon,
		    value                                                    AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), infraconsts.MetricJVMThreadCount)
	var rows []JVMThreadRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "jvm.QueryJVMThreadCountByDaemon", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMMetricNameRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    metric_name AS metric_name,
		    avg(value)  AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		GROUP BY metric_name`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), []string{
		infraconsts.MetricJVMClassLoaded,
		infraconsts.MetricJVMClassCount,
	})
	var rows []JVMMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "jvm.QueryJVMClasses", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMMetricNameRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    metric_name AS metric_name,
		    avg(value)  AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		GROUP BY metric_name`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), []string{
		infraconsts.MetricJVMCPUTime,
		infraconsts.MetricJVMCPUUtilization,
	})
	var rows []JVMMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "jvm.QueryJVMCPU", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryJVMBuffersByPool(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMBufferRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'jvm.buffer.pool.name'::String                AS pool_name,
		    metric_name                                              AS metric_name,
		    value                                                    AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'jvm.buffer.pool.name'::String != ''
		ORDER BY timestamp`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), []string{
		infraconsts.MetricJVMBufferMemoryUsage,
		infraconsts.MetricJVMBufferCount,
	})
	var rows []JVMBufferRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "jvm.QueryJVMBuffersByPool", &rows, query, args...)
}

// ---------------------------------------------------------------------------
// Local helpers — each module owns its own.
// ---------------------------------------------------------------------------

func metricArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func metricBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs /1000) + uint32(timebucket.BucketSeconds)
}

func withMetricName(args []any, name string) []any {
	return append(args, clickhouse.Named("metricName", name))
}

func withMetricNames(args []any, names []string) []any {
	return append(args, clickhouse.Named("metricNames", names))
}
