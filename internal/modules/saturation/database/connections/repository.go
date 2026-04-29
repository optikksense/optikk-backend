// Package connections reads OTel `db.client.connection.*` instrumentation
// metrics from `observability.metrics`. Per the audit, gauges/counters
// are instrumentation-side time series (not per-call spans) and live
// natively on metrics. Every method PREWHEREs raw metrics on
// `(team_id, ts_bucket, fingerprint IN active_fps, metric_name)` —
// full PK granule pruning. Service.go folds raw rows into display
// buckets via `displaybucket` helpers and computes histogram percentiles
// Go-side via `quantile.FromHistogram`.
package connections

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

type Repository interface {
	GetConnectionCountSeries(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]gaugeStateRawDTO, error)
	GetConnectionUtilization(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]gaugeStateRawDTO, error)
	GetConnectionLimits(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]gaugeStateRawDTO, error)
	GetPendingRequests(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]gaugeRawDTO, error)
	GetConnectionTimeoutRate(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]gaugeRawDTO, error)
	GetConnectionWaitTime(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]histRawDTO, error)
	GetConnectionCreateTime(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]histRawDTO, error)
	GetConnectionUseTime(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]histRawDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type gaugeStateRawDTO struct {
	Timestamp  time.Time `ch:"timestamp"`
	MetricName string    `ch:"metric_name"`
	PoolName   string    `ch:"pool_name"`
	State      string    `ch:"state"`
	Value      float64   `ch:"value"`
}

type gaugeRawDTO struct {
	Timestamp time.Time `ch:"timestamp"`
	PoolName  string    `ch:"pool_name"`
	Value     float64   `ch:"value"`
}

type histRawDTO struct {
	Timestamp time.Time `ch:"timestamp"`
	PoolName  string    `ch:"pool_name"`
	Buckets   []float64 `ch:"hist_buckets"`
	Counts    []uint64  `ch:"hist_counts"`
}

// GetConnectionCountSeries reads `db.client.connection.count` (gauge by state).
func (r *ClickHouseRepository) GetConnectionCountSeries(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]gaugeStateRawDTO, error) {
	return r.gaugeWithStateOne(ctx, teamID, startMs, endMs, f, filter.MetricDBConnectionCount, "connections.GetConnectionCountSeries")
}

// GetConnectionUtilization fetches both `count` and `max` together; service
// computes utilization = used/max per (display_bucket, pool).
func (r *ClickHouseRepository) GetConnectionUtilization(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]gaugeStateRawDTO, error) {
	return r.gaugeWithStateMulti(ctx, teamID, startMs, endMs, f,
		[]string{filter.MetricDBConnectionCount, filter.MetricDBConnectionMax},
		"connections.GetConnectionUtilization")
}

// GetConnectionLimits fetches `max` + `idle.max` + `idle.min` together;
// service folds to one row per pool with all three columns.
func (r *ClickHouseRepository) GetConnectionLimits(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]gaugeStateRawDTO, error) {
	return r.gaugeWithStateMulti(ctx, teamID, startMs, endMs, f,
		[]string{filter.MetricDBConnectionMax, filter.MetricDBConnectionIdleMax, filter.MetricDBConnectionIdleMin},
		"connections.GetConnectionLimits")
}

func (r *ClickHouseRepository) GetPendingRequests(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]gaugeRawDTO, error) {
	return r.gaugeNoState(ctx, teamID, startMs, endMs, f, filter.MetricDBConnectionPendReqs, "connections.GetPendingRequests")
}

// GetConnectionTimeoutRate reads `db.client.connection.timeouts` (counter);
// service Go-side folds into per-(display_bucket, pool) sums and divides by
// display-grain seconds for per-second rate.
func (r *ClickHouseRepository) GetConnectionTimeoutRate(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]gaugeRawDTO, error) {
	return r.gaugeNoState(ctx, teamID, startMs, endMs, f, filter.MetricDBConnectionTimeouts, "connections.GetConnectionTimeoutRate")
}

func (r *ClickHouseRepository) GetConnectionWaitTime(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]histRawDTO, error) {
	return r.poolHistogram(ctx, teamID, startMs, endMs, f, filter.MetricDBConnectionWaitTime, "connections.GetConnectionWaitTime")
}

func (r *ClickHouseRepository) GetConnectionCreateTime(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]histRawDTO, error) {
	return r.poolHistogram(ctx, teamID, startMs, endMs, f, filter.MetricDBConnectionCreateTime, "connections.GetConnectionCreateTime")
}

func (r *ClickHouseRepository) GetConnectionUseTime(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]histRawDTO, error) {
	return r.poolHistogram(ctx, teamID, startMs, endMs, f, filter.MetricDBConnectionUseTime, "connections.GetConnectionUseTime")
}

// ---------------------------------------------------------------------------
// Internal query templates — three shapes (gauge-with-state, gauge-no-state,
// histogram). Stable bind names so identical predicate sets produce
// byte-identical SQL.
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) gaugeWithStateOne(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, metricName, traceLabel string) ([]gaugeStateRawDTO, error) {
	filterWhere, filterArgs := filter.BuildMetricClauses(f)
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name = @metricName
		)
		SELECT timestamp                                                       AS timestamp,
		       metric_name                                                     AS metric_name,
		       attributes.'pool.name'::String                                  AS pool_name,
		       attributes.'db.client.connection.state'::String                 AS state,
		       value                                                           AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint    IN active_fps
		     AND metric_name    = @metricName
		WHERE timestamp BETWEEN @start AND @end`
	args := append(filter.MetricArgs(teamID, startMs, endMs, metricName), filterArgs...)
	full := query + filterWhere
	var rows []gaugeStateRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, traceLabel, &rows, full, args...)
}

func (r *ClickHouseRepository) gaugeWithStateMulti(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, metricNames []string, traceLabel string) ([]gaugeStateRawDTO, error) {
	filterWhere, filterArgs := filter.BuildMetricClauses(f)
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name IN @metricNames
		)
		SELECT timestamp                                                       AS timestamp,
		       metric_name                                                     AS metric_name,
		       attributes.'pool.name'::String                                  AS pool_name,
		       attributes.'db.client.connection.state'::String                 AS state,
		       value                                                           AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint    IN active_fps
		     AND metric_name    IN @metricNames
		WHERE timestamp BETWEEN @start AND @end`
	args := append(filter.MetricArgsMulti(teamID, startMs, endMs, metricNames), filterArgs...)
	full := query + filterWhere
	var rows []gaugeStateRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, traceLabel, &rows, full, args...)
}

func (r *ClickHouseRepository) gaugeNoState(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, metricName, traceLabel string) ([]gaugeRawDTO, error) {
	filterWhere, filterArgs := filter.BuildMetricClauses(f)
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name = @metricName
		)
		SELECT timestamp                              AS timestamp,
		       attributes.'pool.name'::String          AS pool_name,
		       value                                   AS value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint    IN active_fps
		     AND metric_name    = @metricName
		WHERE timestamp BETWEEN @start AND @end`
	args := append(filter.MetricArgs(teamID, startMs, endMs, metricName), filterArgs...)
	full := query + filterWhere
	var rows []gaugeRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, traceLabel, &rows, full, args...)
}

func (r *ClickHouseRepository) poolHistogram(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, metricName, traceLabel string) ([]histRawDTO, error) {
	filterWhere, filterArgs := filter.BuildMetricClauses(f)
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name = @metricName
		)
		SELECT timestamp                              AS timestamp,
		       attributes.'pool.name'::String          AS pool_name,
		       hist_buckets                            AS hist_buckets,
		       hist_counts                             AS hist_counts
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint    IN active_fps
		     AND metric_name    = @metricName
		WHERE timestamp BETWEEN @start AND @end
		  AND metric_type = 'Histogram'`
	args := append(filter.MetricArgs(teamID, startMs, endMs, metricName), filterArgs...)
	full := query + filterWhere
	var rows []histRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, traceLabel, &rows, full, args...)
}
