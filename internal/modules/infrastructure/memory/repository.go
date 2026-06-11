package memory

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
	"github.com/Optikk-Org/optikk-backend/internal/shared/chargs"
)

// All read paths query metrics by joining metrics_resource on fingerprint.

var memMetricNames = []string{
	infraconsts.MetricSystemMemoryUtilization,
	infraconsts.MetricSystemMemoryUsage,
	infraconsts.MetricJVMMemoryUsed,
	infraconsts.MetricJVMMemoryMax,
}

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

func (r *Repository) QueryMemoryUtilizationAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]MemoryMetricNameRow, error) {
	query := `
		SELECT
		    metric_name AS metric_name,
		    sum(val_sum) / sum(val_count)  AS value
		FROM ` + timebucket.MetricsRollup(endMs-startMs) + `
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name   IN @metricNames
		     AND timestamp   BETWEEN @start AND @end
		GROUP BY metric_name`
	args := chargs.WithMetricNames(chargs.RollupRangeArgs(teamID, startMs, endMs), memMetricNames)
	var rows []MemoryMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.QueryMemoryUtilizationAgg", &rows, query, args...)
}

func (r *Repository) QueryMemoryUtilizationForInstance(ctx context.Context, teamID int64, startMs, endMs int64, host, pod, serviceName string) ([]MemoryMetricNameRow, error) {
	// Filter resolves to a fingerprint set via metrics_resource,
	// then narrows the scalar rollup by fingerprint.
	query := `
		WITH fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		    WHERE host = @host AND service = @serviceName AND pod = @pod
		)
		SELECT
		    metric_name AS metric_name,
		    sum(val_sum) / sum(val_count)  AS value
		FROM ` + timebucket.MetricsRollup(endMs-startMs) + `
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name   IN @metricNames
		     AND timestamp   BETWEEN @start AND @end
		WHERE fingerprint IN fps
		GROUP BY metric_name`
	args := chargs.WithMetricNames(chargs.RollupRangeArgs(teamID, startMs, endMs), memMetricNames)
	args = append(args,
		clickhouse.Named("host", host),
		clickhouse.Named("pod", pod),
		clickhouse.Named("serviceName", serviceName),
	)
	var rows []MemoryMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.QueryMemoryUtilizationForInstance", &rows, query, args...)
}
