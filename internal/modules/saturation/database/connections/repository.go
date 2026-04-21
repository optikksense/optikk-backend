package connections

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetConnectionCountSeries(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionCountPoint, error)
	GetConnectionUtilization(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionUtilPoint, error)
	GetConnectionLimits(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionLimits, error)
	GetPendingRequests(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PendingRequestsPoint, error)
	GetConnectionTimeoutRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionTimeoutPoint, error)
	GetConnectionWaitTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error)
	GetConnectionCreateTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error)
	GetConnectionUseTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetConnectionCountSeries(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionCountPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := shared.AttrString(shared.AttrPoolName)
	stateAttr := shared.AttrString(shared.AttrConnectionState)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s               AS time_bucket,
		    %s               AS pool_name,
		    %s               AS state,
		    avg(value)       AS count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  %s
		GROUP BY time_bucket, pool_name, state
		ORDER BY time_bucket, pool_name, state
	`,
		bucket, poolAttr, stateAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionCount,
		fc,
	)

	var rows []ConnectionCountPoint
	args := append(shared.BaseParams(teamID, startMs, endMs), fargs...)
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetConnectionUtilization(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionUtilPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := shared.AttrString(shared.AttrPoolName)
	stateAttr := shared.AttrString(shared.AttrConnectionState)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                           AS time_bucket,
		    %s                                                           AS pool_name,
		    avgIf(value, %s = 'used')                                   AS used_avg,
		    (
		        SELECT avg(value)
		        FROM %s AS mx
		        WHERE mx.%s = @teamID
		          AND mx.%s BETWEEN @start AND @end
		          AND mx.%s = '%s'
		          AND mx.%s = %s
		          %s
		    )                                                             AS max_val,
		    if(max_val > 0, used_avg / max_val * 100, NULL)             AS util_pct
		FROM %s AS m
		WHERE m.%s = @teamID
		  AND m.%s BETWEEN @start AND @end
		  AND m.%s = '%s'
		  %s
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`,
		bucket, poolAttr, stateAttr,
		shared.TableMetrics,
		shared.ColTeamID,
		shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionMax,
		poolAttr, poolAttr,
		fc,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionCount,
		fc,
	)

	var rows []ConnectionUtilPoint
	args := append(shared.BaseParams(teamID, startMs, endMs), fargs...)
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetConnectionLimits(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionLimits, error) {
	poolAttr := shared.AttrString(shared.AttrPoolName)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                                               AS pool_name,
		    avgIf(value, metric_name = '%s')                AS max_val,
		    avgIf(value, metric_name = '%s')                AS idle_max,
		    avgIf(value, metric_name = '%s')                AS idle_min
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s', '%s')
		  %s
		GROUP BY pool_name
		ORDER BY pool_name
	`,
		poolAttr,
		shared.MetricDBConnectionMax, shared.MetricDBConnectionIdleMax, shared.MetricDBConnectionIdleMin,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName,
		shared.MetricDBConnectionMax, shared.MetricDBConnectionIdleMax, shared.MetricDBConnectionIdleMin,
		fc,
	)

	var rows []ConnectionLimits
	args := append(shared.BaseParams(teamID, startMs, endMs), fargs...)
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetPendingRequests(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PendingRequestsPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := shared.AttrString(shared.AttrPoolName)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s             AS time_bucket,
		    %s             AS pool_name,
		    avg(value)     AS count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  %s
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`,
		bucket, poolAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionPendReqs,
		fc,
	)

	var rows []PendingRequestsPoint
	args := append(shared.BaseParams(teamID, startMs, endMs), fargs...)
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetConnectionTimeoutRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionTimeoutPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := shared.AttrString(shared.AttrPoolName)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                               AS time_bucket,
		    %s                               AS pool_name,
		    toFloat64(sum(value)) / %f        AS timeout_rate
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  %s
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`,
		bucket, poolAttr,
		bucketSec,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionTimeouts,
		fc,
	)

	var rows []ConnectionTimeoutPoint
	args := append(shared.BaseParams(teamID, startMs, endMs), fargs...)
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) poolLatency(ctx context.Context, teamID int64, startMs, endMs int64, metricName string, f shared.Filters) ([]PoolLatencyPoint, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)
	fc, fargs := shared.RollupFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    pool_name                                                                   AS pool_name,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 * 1000  AS p50_ms,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 * 1000  AS p95_ms,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 * 1000  AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`, shared.BucketTimeExpr, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, metricName),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)
	var rows []PoolLatencyPoint
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetConnectionWaitTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error) {
	return r.poolLatency(ctx, teamID, startMs, endMs, shared.MetricDBConnectionWaitTime, f)
}

func (r *ClickHouseRepository) GetConnectionCreateTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error) {
	return r.poolLatency(ctx, teamID, startMs, endMs, shared.MetricDBConnectionCreateTime, f)
}

func (r *ClickHouseRepository) GetConnectionUseTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error) {
	return r.poolLatency(ctx, teamID, startMs, endMs, shared.MetricDBConnectionUseTime, f)
}
