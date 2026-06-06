package memory

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

// All read paths query metrics by joining metrics_resource on fingerprint.

var memMetricNames = []string{
	infraconsts.MetricSystemMemoryUtilization,
	infraconsts.MetricSystemMemoryUsage,
	infraconsts.MetricJVMMemoryUsed,
	infraconsts.MetricJVMMemoryMax,
}

type Repository interface {
	QueryMemoryUtilizationAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]MemoryMetricNameRow, error)
	QueryMemoryUtilizationForInstance(ctx context.Context, teamID int64, startMs, endMs int64, host, pod, serviceName string) ([]MemoryMetricNameRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) QueryMemoryUtilizationAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]MemoryMetricNameRow, error) {
	const query = `
		SELECT
		    metric_name AS metric_name,
		    sum(val_sum) / sum(val_count)  AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name   IN @metricNames
		     AND timestamp   BETWEEN @start AND @end
		GROUP BY metric_name`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), memMetricNames)
	var rows []MemoryMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.QueryMemoryUtilizationAgg", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryMemoryUtilizationForInstance(ctx context.Context, teamID int64, startMs, endMs int64, host, pod, serviceName string) ([]MemoryMetricNameRow, error) {
	// Filter resolves to a fingerprint set via metrics_resource,
	// then narrows the scalar rollup by fingerprint.
	const query = `
		WITH fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		    WHERE host = @host AND service = @serviceName AND pod = @pod
		)
		SELECT
		    metric_name AS metric_name,
		    sum(val_sum) / sum(val_count)  AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name   IN @metricNames
		     AND timestamp   BETWEEN @start AND @end
		WHERE fingerprint IN fps
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

// Local helpers.

func metricArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
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
