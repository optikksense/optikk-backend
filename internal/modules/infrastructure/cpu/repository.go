package cpu

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
	"github.com/Optikk-Org/optikk-backend/internal/shared/chargs"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

func (r *Repository) QueryCPUUtilizationAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUMetricNameRow, error) {
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
	args := chargs.WithMetricNames(chargs.RangeArgs(teamID, startMs, endMs), infraconsts.CPUMetrics)
	var rows []CPUMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.QueryCPUUtilizationAgg", &rows, query, args...)
}

func (r *Repository) QueryCPUUtilizationByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUInstanceMetricRow, error) {
	// Resource dims (host/pod/container/service) live in metrics_resource;
	// resolve them per fingerprint and join the scalar rollup on fingerprint.
	const query = `
		WITH fps AS (
		    SELECT fingerprint,
		           any(host)      AS host,
		           any(pod)       AS pod,
		           any(container) AS container,
		           any(service)   AS service
		    FROM observability.metrics_resource AS mr
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		    WHERE mr.service != ''
		    GROUP BY fingerprint
		)
		SELECT
		    r.host                        AS host,
		    r.pod                         AS pod,
		    r.container                   AS container,
		    r.service                     AS service,
		    m.metric_name                 AS metric_name,
		    sum(m.val_sum) / sum(m.val_count) AS value
		FROM observability.metrics_1m AS m
		INNER JOIN fps AS r ON m.fingerprint = r.fingerprint
		PREWHERE m.team_id     = @teamID
		     AND m.ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND m.metric_name IN @metricNames
		     AND m.timestamp   BETWEEN @start AND @end
		GROUP BY host, pod, container, service, metric_name
		ORDER BY service, pod`
	args := chargs.WithMetricNames(chargs.RangeArgs(teamID, startMs, endMs), infraconsts.CPUMetrics)
	var rows []CPUInstanceMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.QueryCPUUtilizationByInstance", &rows, query, args...)
}
