package memory

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
// PREWHEREs on (team_id, ts_bucket, fingerprint). Service composes the
// 4-metric memory fold (system.memory.utilization + system.memory.usage +
// jvm.memory.used + jvm.memory.max).

var memMetricNames = []string{
	infraconsts.MetricSystemMemoryUtilization,
	infraconsts.MetricSystemMemoryUsage,
	infraconsts.MetricJVMMemoryUsed,
	infraconsts.MetricJVMMemoryMax,
}

type Repository interface {
	QueryMemoryUtilizationAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]MemoryMetricNameRow, error)
	QueryMemoryUtilizationForInstance(ctx context.Context, teamID int64, startMs, endMs int64, host, pod, serviceName string) ([]MemoryMetricNameRow, error)
	QueryMemoryByHost(ctx context.Context, teamID int64, startMs, endMs int64) ([]MemoryHostMetricRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) QueryMemoryUtilizationAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]MemoryMetricNameRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), memMetricNames)
	var rows []MemoryMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.QueryMemoryUtilizationAgg", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryMemoryUtilizationForInstance(ctx context.Context, teamID int64, startMs, endMs int64, host, pod, serviceName string) ([]MemoryMetricNameRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), memMetricNames)
	args = append(args,
		clickhouse.Named("host", host),
		clickhouse.Named("pod", pod),
		clickhouse.Named("serviceName", serviceName),
	)
	var rows []MemoryMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.QueryMemoryUtilizationForInstance", &rows, query, args...)
}

// QueryMemoryByHost returns per-(host, metric) averages across the window, one
// row per metric per host. Service folds the 4-metric memory blend per host,
// then ranks DESC and limits — the blend is Go-side, so the top-N ranking
// cannot be pushed into SQL. Mirrors QueryMemoryUtilizationAgg with host as a
// group dimension.
func (r *ClickHouseRepository) QueryMemoryByHost(ctx context.Context, teamID int64, startMs, endMs int64) ([]MemoryHostMetricRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), memMetricNames)
	var rows []MemoryHostMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.QueryMemoryByHost", &rows, query, args...)
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
