package cpu

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	QueryCPUTimeByState(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUStateRow, error)
	QueryProcessCountByState(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUStateRow, error)
	QueryCPUUtilizationByPod(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUPodMetricRow, error)
	QueryLoadAverages(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUMetricNameRow, error)
	QueryCPUUtilizationAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUMetricNameRow, error)
	QueryCPUUtilizationByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUServiceMetricRow, error)
	QueryCPUUtilizationByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUInstanceMetricRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) QueryCPUTimeByState(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUStateRow, error) {
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
		    attributes.'system.cpu.state'::String                    AS state,
		    value                                                    AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'system.cpu.state'::String != ''
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), infraconsts.MetricSystemCPUTime)
	var rows []CPUStateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.QueryCPUTimeByState", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryProcessCountByState(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUStateRow, error) {
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
		    attributes.'process.status'::String                      AS state,
		    value                                                    AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'process.status'::String != ''
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), infraconsts.MetricSystemProcessCount)
	var rows []CPUStateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.QueryProcessCountByState", &rows, query, args...)
}

// QueryCPUUtilizationByPod returns per-(timestamp, pod, metric_name) average
// value for the 3-metric utilization family. Service folds the metrics into a
// single percentage per (timestamp, pod) and applies the ≤1.0 → *100 norm.
func (r *ClickHouseRepository) QueryCPUUtilizationByPod(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUPodMetricRow, error) {
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
		    attributes.'k8s.pod.name'::String                        AS pod,
		    metric_name                                              AS metric_name,
		    value                                                    AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'k8s.pod.name'::String != ''
		ORDER BY timestamp`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), infraconsts.CPUMetrics)
	var rows []CPUPodMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.QueryCPUUtilizationByPod", &rows, query, args...)
}

// QueryLoadAverages returns one row per load-average metric (1m/5m/15m) with
// the average value across the window.
func (r *ClickHouseRepository) QueryLoadAverages(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUMetricNameRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), []string{
		infraconsts.MetricSystemCPULoadAvg1m,
		infraconsts.MetricSystemCPULoadAvg5m,
		infraconsts.MetricSystemCPULoadAvg15m,
	})
	var rows []CPUMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.QueryLoadAverages", &rows, query, args...)
}

// QueryCPUUtilizationAgg returns one row per CPU-utilization metric with the
// average value across the window. Service folds the 3 metrics into a single
// dashboard percentage.
func (r *ClickHouseRepository) QueryCPUUtilizationAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUMetricNameRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), infraconsts.CPUMetrics)
	var rows []CPUMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.QueryCPUUtilizationAgg", &rows, query, args...)
}

// QueryCPUUtilizationByService returns per-(service, metric_name) averages for
// the 3-metric utilization family. Service folds + normalizes.
func (r *ClickHouseRepository) QueryCPUUtilizationByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUServiceMetricRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    service     AS service,
		    metric_name AS metric_name,
		    avg(value)  AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND service != ''
		GROUP BY service, metric_name
		ORDER BY service`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), infraconsts.CPUMetrics)
	var rows []CPUServiceMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.QueryCPUUtilizationByService", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryCPUUtilizationByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUInstanceMetricRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    host                                                     AS host,
		    attributes.'k8s.pod.name'::String                        AS pod,
		    attributes.'k8s.container.name'::String                  AS container,
		    service                                                  AS service,
		    metric_name                                              AS metric_name,
		    avg(value)                                               AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND service != ''
		GROUP BY host, pod, container, service, metric_name
		ORDER BY service, pod`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), infraconsts.CPUMetrics)
	var rows []CPUInstanceMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.QueryCPUUtilizationByInstance", &rows, query, args...)
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
