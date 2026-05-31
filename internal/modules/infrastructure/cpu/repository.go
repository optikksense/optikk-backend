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
	QueryCPUUtilizationAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUMetricNameRow, error)
	QueryCPUUtilizationByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUInstanceMetricRow, error)
	QueryCPUByHost(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUHostMetricRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
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
		    sum(val_sum) / sum(val_count)  AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		GROUP BY metric_name`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), infraconsts.CPUMetrics)
	var rows []CPUMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.QueryCPUUtilizationAgg", &rows, query, args...)
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
		    sum(val_sum) / sum(val_count)                                               AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
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

// QueryCPUByHost returns per-(host, metric) averages across the window, one
// row per metric per host. Service folds the 3-metric blend per host, then
// ranks DESC and limits — the multi-metric percentage blend is Go-side, so the
// top-N ranking cannot be pushed into SQL. Mirrors QueryCPUUtilizationByInstance
// collapsed to host granularity.
func (r *ClickHouseRepository) QueryCPUByHost(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUHostMetricRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    host                           AS host,
		    metric_name                    AS metric_name,
		    sum(val_sum) / sum(val_count)  AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND host != ''
		GROUP BY host, metric_name`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), infraconsts.CPUMetrics)
	var rows []CPUHostMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.QueryCPUByHost", &rows, query, args...)
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

func withMetricNames(args []any, names []string) []any {
	return append(args, clickhouse.Named("metricNames", names))
}
