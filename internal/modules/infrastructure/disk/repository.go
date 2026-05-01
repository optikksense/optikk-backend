package disk

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

// All read paths follow the apm/httpmetrics pattern: `WITH active_fps AS
// (... metrics_resource ...)` CTE so the main `observability.metrics` scan
// PREWHEREs on (team_id, ts_bucket, fingerprint). One CH call per method;
// service.go composes panels.

type Repository interface {
	QueryDiskIOByDirection(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskDirectionRow, error)
	QueryDiskOperationsByDirection(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskDirectionRow, error)
	QueryDiskIOTimeTotal(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskValueRow, error)
	QueryFilesystemUsageByMountpoint(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskMountpointRow, error)
	QueryFilesystemUtilizationTotal(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskValueRow, error)
	QueryDiskUtilizationAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskMetricNameRow, error)
	QueryDiskUtilizationForService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]DiskMetricNameRow, error)
	QueryDiskUtilizationForInstance(ctx context.Context, teamID int64, startMs, endMs int64, host, pod, serviceName string) ([]DiskMetricNameRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

const diskCounterByDirectionQuery = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'system.disk.direction'::String               AS direction,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'system.disk.direction'::String != ''
		ORDER BY timestamp`

func (r *ClickHouseRepository) queryDiskCounterByDirection(ctx context.Context, op, metricName string, teamID int64, startMs, endMs int64) ([]DiskDirectionRow, error) {
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var rows []DiskDirectionRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, op, &rows, diskCounterByDirectionQuery, args...)
}

func (r *ClickHouseRepository) QueryDiskIOByDirection(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskDirectionRow, error) {
	return r.queryDiskCounterByDirection(ctx, "disk.QueryDiskIOByDirection", infraconsts.MetricSystemDiskIO, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryDiskOperationsByDirection(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskDirectionRow, error) {
	return r.queryDiskCounterByDirection(ctx, "disk.QueryDiskOperationsByDirection", infraconsts.MetricSystemDiskOperations, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryDiskIOTimeTotal(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskValueRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    timestamp AS timestamp,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), infraconsts.MetricSystemDiskIOTime)
	var rows []DiskValueRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "disk.QueryDiskIOTimeTotal", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryFilesystemUsageByMountpoint(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskMountpointRow, error) {
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
		    attributes.'system.filesystem.mountpoint'::String        AS mountpoint,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'system.filesystem.mountpoint'::String != ''
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), infraconsts.MetricSystemFilesystemUsage)
	var rows []DiskMountpointRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "disk.QueryFilesystemUsageByMountpoint", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryFilesystemUtilizationTotal(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskValueRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    timestamp AS timestamp,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), infraconsts.MetricSystemFilesystemUtil)
	var rows []DiskValueRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "disk.QueryFilesystemUtilizationTotal", &rows, query, args...)
}

// QueryDiskUtilizationAgg returns one row per disk-utilization metric with the
// average value across the window. Service folds the 3 metrics (utilization +
// free + total) into a single dashboard percentage.
func (r *ClickHouseRepository) QueryDiskUtilizationAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]DiskMetricNameRow, error) {
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
		    sum(val_sum) / sum(val_count)  AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		GROUP BY metric_name`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), infraconsts.DiskMetrics)
	var rows []DiskMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "disk.QueryDiskUtilizationAgg", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryDiskUtilizationForService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]DiskMetricNameRow, error) {
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
		    sum(val_sum) / sum(val_count)  AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND service = @serviceName
		GROUP BY metric_name`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), infraconsts.DiskMetrics)
	args = append(args, clickhouse.Named("serviceName", serviceName))
	var rows []DiskMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "disk.QueryDiskUtilizationForService", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryDiskUtilizationForInstance(ctx context.Context, teamID int64, startMs, endMs int64, host, pod, serviceName string) ([]DiskMetricNameRow, error) {
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
		    sum(val_sum) / sum(val_count)  AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND host    = @host
		  AND service = @serviceName
		  AND attributes.'k8s.pod.name'::String = @pod
		GROUP BY metric_name`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), infraconsts.DiskMetrics)
	args = append(args,
		clickhouse.Named("host", host),
		clickhouse.Named("pod", pod),
		clickhouse.Named("serviceName", serviceName),
	)
	var rows []DiskMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "disk.QueryDiskUtilizationForInstance", &rows, query, args...)
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
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}

func withMetricName(args []any, name string) []any {
	return append(args, clickhouse.Named("metricName", name))
}

func withMetricNames(args []any, names []string) []any {
	return append(args, clickhouse.Named("metricNames", names))
}
