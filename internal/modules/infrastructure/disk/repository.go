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
// PREWHEREs on (team_id, ts_bucket_hour, fingerprint). One CH call per method;
// service.go composes panels.

type Repository interface {
	QueryDiskCounterByDirection(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]DiskDirectionRow, error)
	QueryDiskCounterTotal(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]DiskValueRow, error)
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

func (r *ClickHouseRepository) QueryDiskCounterByDirection(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]DiskDirectionRow, error) {
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
		    attributes.'system.disk.direction'::String               AS direction,
		    value                                                    AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'system.disk.direction'::String != ''
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var rows []DiskDirectionRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "disk.QueryDiskCounterByDirection", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryDiskCounterTotal(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]DiskValueRow, error) {
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
		    value     AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var rows []DiskValueRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "disk.QueryDiskCounterTotal", &rows, query, args...)
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
		    value                                                    AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
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
		    value     AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
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
		    avg(value)  AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
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
		    avg(value)  AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
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
		    avg(value)  AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
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

func metricBucketBounds(startMs, endMs int64) (time.Time, time.Time) {
	return timebucket.MetricsHourBucket(startMs / 1000),
		timebucket.MetricsHourBucket(endMs / 1000).Add(time.Hour)
}

func withMetricName(args []any, name string) []any {
	return append(args, clickhouse.Named("metricName", name))
}

func withMetricNames(args []any, names []string) []any {
	return append(args, clickhouse.Named("metricNames", names))
}
