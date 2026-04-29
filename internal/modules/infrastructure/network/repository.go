package network

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	QueryNetworkCounterByDirection(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]NetworkDirectionRow, error)
	QueryNetworkCounterTotal(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]NetworkValueRow, error)
	QueryNetworkGaugeByState(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]NetworkStateRow, error)
	QueryNetworkUtilizationByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]NetworkServiceRow, error)
	QueryNetworkUtilizationForService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (NetworkScalarRow, error)
	QueryNetworkUtilizationForInstance(ctx context.Context, teamID int64, startMs, endMs int64, host, pod, serviceName string) (NetworkScalarRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) QueryNetworkCounterByDirection(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]NetworkDirectionRow, error) {
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
		    attributes.'system.network.io.direction'::String         AS direction,
		    value                                                    AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'system.network.io.direction'::String != ''
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var rows []NetworkDirectionRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.QueryNetworkCounterByDirection", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryNetworkCounterTotal(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]NetworkValueRow, error) {
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
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var rows []NetworkValueRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.QueryNetworkCounterTotal", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryNetworkGaugeByState(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]NetworkStateRow, error) {
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
		    attributes.'system.network.state'::String                AS state,
		    value                                                    AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'system.network.state'::String != ''
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var rows []NetworkStateRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.QueryNetworkGaugeByState", &rows, query, args...)
}

// QueryNetworkUtilizationByService returns one row per service with that
// service's average system.network.utilization across the window. Service
// applies the ≤1.0 → *100 normalization and averages across services.
func (r *ClickHouseRepository) QueryNetworkUtilizationByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]NetworkServiceRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    service     AS service,
		    avg(value)  AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND service != ''
		GROUP BY service
		HAVING value IS NOT NULL`
	args := withMetricName(metricArgs(teamID, startMs, endMs), infraconsts.MetricSystemNetworkUtilization)
	var rows []NetworkServiceRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.QueryNetworkUtilizationByService", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryNetworkUtilizationForService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (NetworkScalarRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT avg(value) AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND service = @serviceName`
	args := withMetricName(metricArgs(teamID, startMs, endMs), infraconsts.MetricSystemNetworkUtilization)
	args = append(args, clickhouse.Named("serviceName", serviceName))
	var row NetworkScalarRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "network.QueryNetworkUtilizationForService", &row, query, args...)
}

func (r *ClickHouseRepository) QueryNetworkUtilizationForInstance(ctx context.Context, teamID int64, startMs, endMs int64, host, pod, serviceName string) (NetworkScalarRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT avg(value) AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND host    = @host
		  AND service = @serviceName
		  AND attributes.'k8s.pod.name'::String = @pod`
	args := withMetricName(metricArgs(teamID, startMs, endMs), infraconsts.MetricSystemNetworkUtilization)
	args = append(args,
		clickhouse.Named("host", host),
		clickhouse.Named("pod", pod),
		clickhouse.Named("serviceName", serviceName),
	)
	var row NetworkScalarRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "network.QueryNetworkUtilizationForInstance", &row, query, args...)
}

// ---------------------------------------------------------------------------
// Local helpers — each module owns its own (per plan, no shared helpers).
// ---------------------------------------------------------------------------

func metricArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — TeamID fits UInt32
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
