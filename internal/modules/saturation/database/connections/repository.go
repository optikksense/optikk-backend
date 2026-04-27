package connections

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
		timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

// Connection panels read `db_histograms_rollup` which carries:
//   - `pool_name` + `db_connection_state` as keys (v2 additions)
//   - `value_sum` / `sample_count` / `value_last` state (v2 gauge rows)
//   - `latency_ms_digest` / `hist_count` / `hist_sum` state (v1 histogram)
// so every method below runs through the rollup.

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

func bucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumn(startMs, endMs, "bucket_ts")
}

func (r *ClickHouseRepository) GetConnectionCountSeries(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionCountPoint, error) {
	table := "observability.signoz_index_v3"
	fc, fargs := "", []any{}
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    pool_name                                                                   AS pool_name,
		    db_connection_state                                                         AS state,
		    sum(value_sum) / nullIf(toFloat64(sum(sample_count)), 0)          AS count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, pool_name, state
		ORDER BY time_bucket, pool_name, state
	`, bucketExpr(startMs, endMs), table, fc)
	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBConnectionCount), fargs...)
	var rows []ConnectionCountPoint
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connections.GetConnectionCountSeries", &rows, query, args...)
}

// GetConnectionUtilization folds `used` (state=used) vs `max` (metric_name=
// db.client.connection.max) per bucket+pool in Go. Previously did a
// correlated subquery against raw metrics.
func (r *ClickHouseRepository) GetConnectionUtilization(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionUtilPoint, error) {
	table := "observability.signoz_index_v3"
	fc, fargs := "", []any{}
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    pool_name                                                                   AS pool_name,
		    metric_name                                                                 AS metric_name,
		    db_connection_state                                                         AS state,
		    sum(value_sum) / nullIf(toFloat64(sum(sample_count)), 0)          AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN (@countMetric, @maxMetric)
		  %s
		GROUP BY time_bucket, pool_name, metric_name, state
	`, bucketExpr(startMs, endMs), table, fc)
	args := append(shared.BaseParams(teamID, startMs, endMs),
		clickhouse.Named("countMetric", shared.MetricDBConnectionCount),
		clickhouse.Named("maxMetric", shared.MetricDBConnectionMax),
	)
	args = append(args, fargs...)
	var metricRows []struct {
		TimeBucket string  `ch:"time_bucket"`
		PoolName   string  `ch:"pool_name"`
		MetricName string  `ch:"metric_name"`
		State      string  `ch:"state"`
		ValAvg     float64 `ch:"val_avg"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connections.GetConnectionUtilization", &metricRows, query, args...); err != nil {
		return nil, err
	}
	type key struct{ bucket, pool string }
	type agg struct {
		used, max *float64
	}
	folded := map[key]*agg{}
	for _, mr := range metricRows {
		k := key{mr.TimeBucket, mr.PoolName}
		a, ok := folded[k]
		if !ok {
			a = &agg{}
			folded[k] = a
		}
		v := mr.ValAvg
		switch {
		case mr.MetricName == shared.MetricDBConnectionCount && mr.State == "used":
			a.used = &v
		case mr.MetricName == shared.MetricDBConnectionMax:
			a.max = &v
		}
	}
	rows := make([]ConnectionUtilPoint, 0, len(folded))
	for k, a := range folded {
		pt := ConnectionUtilPoint{TimeBucket: k.bucket, PoolName: k.pool}
		if a.used != nil && a.max != nil && *a.max > 0 {
			u := *a.used / *a.max * 100.0
			pt.UtilPct = &u
		}
		rows = append(rows, pt)
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetConnectionLimits(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionLimits, error) {
	table := "observability.signoz_index_v3"
	fc, fargs := "", []any{}
	query := fmt.Sprintf(`
		SELECT
		    pool_name                                                                   AS pool_name,
		    metric_name                                                                 AS metric_name,
		    sum(value_sum) / nullIf(toFloat64(sum(sample_count)), 0)          AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN (@maxMetric, @idleMax, @idleMin)
		  %s
		GROUP BY pool_name, metric_name
		ORDER BY pool_name
	`, table, fc)
	args := append(shared.BaseParams(teamID, startMs, endMs),
		clickhouse.Named("maxMetric", shared.MetricDBConnectionMax),
		clickhouse.Named("idleMax", shared.MetricDBConnectionIdleMax),
		clickhouse.Named("idleMin", shared.MetricDBConnectionIdleMin),
	)
	args = append(args, fargs...)
	var metricRows []struct {
		PoolName   string  `ch:"pool_name"`
		MetricName string  `ch:"metric_name"`
		ValAvg     float64 `ch:"val_avg"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connections.GetConnectionLimits", &metricRows, query, args...); err != nil {
		return nil, err
	}
	out := map[string]*ConnectionLimits{}
	for _, mr := range metricRows {
		lim, ok := out[mr.PoolName]
		if !ok {
			lim = &ConnectionLimits{PoolName: mr.PoolName}
			out[mr.PoolName] = lim
		}
		v := mr.ValAvg
		switch mr.MetricName {
		case shared.MetricDBConnectionMax:
			lim.Max = &v
		case shared.MetricDBConnectionIdleMax:
			lim.IdleMax = &v
		case shared.MetricDBConnectionIdleMin:
			lim.IdleMin = &v
		}
	}
	rows := make([]ConnectionLimits, 0, len(out))
	for _, lim := range out {
		rows = append(rows, *lim)
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetPendingRequests(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PendingRequestsPoint, error) {
	table := "observability.signoz_index_v3"
	fc, fargs := shared.RollupFilterClauses(f)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    pool_name                                                                   AS pool_name,
		    sum(value_sum) / nullIf(toFloat64(sum(sample_count)), 0)          AS count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`, bucketExpr(startMs, endMs), table, fc)
	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBConnectionPendReqs), fargs...)
	var rows []PendingRequestsPoint
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connections.GetPendingRequests", &rows, query, args...)
}

func (r *ClickHouseRepository) GetConnectionTimeoutRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionTimeoutPoint, error) {
	table := "observability.signoz_index_v3"
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	fc, fargs := shared.RollupFilterClauses(f)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                  AS time_bucket,
		    pool_name                                           AS pool_name,
		    toFloat64(sum(value_sum)) / %f                 AS timeout_rate
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`, bucketExpr(startMs, endMs), bucketSec, table, fc)
	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBConnectionTimeouts), fargs...)
	var rows []ConnectionTimeoutPoint
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connections.GetConnectionTimeoutRate", &rows, query, args...)
}

func (r *ClickHouseRepository) poolLatency(ctx context.Context, teamID int64, startMs, endMs int64, metricName string, f shared.Filters) ([]PoolLatencyPoint, error) {
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	fc, fargs := shared.RollupFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    pool_name                                                                   AS pool_name,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1]) * 1000  AS p50_ms,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) * 1000  AS p95_ms,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) * 1000  AS p99_ms
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
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connections.poolLatency", &rows, query, args...); err != nil {
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
