// Package connections reads OTel `db.client.connection.*` instrumentation
// metrics from `observability.metrics_1m`. Per the audit, gauges/counters
// are instrumentation-side time series (not per-call spans) and live
// natively on metrics. Every method PREWHEREs metrics_1m on
// `(team_id, ts_bucket, fingerprint IN active_fps, metric_name)` —
// full PK granule pruning. Histogram percentiles come from
// `quantilesPrometheusHistogramMerge(...)(latency_state)` server-side,
// bucketed at the window-adaptive display grain via
// `timebucket.DisplayGrainSQL` (replaces the prior hardcoded
// `toStartOfHour` which collapsed short windows to a single bucket).
// service.go folds raw 1-min gauge rows into display buckets via a
// per-(bucket, pool[, state]) map keyed on `bucketStr(timestamp)`.
package connections

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
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

// histRawDTO carries server-side p50/p95/p99 (seconds, via
// quantilePrometheusHistogramMerge on metrics_1m.latency_state) per
// (hour-bucket, pool). Service multiplies by 1000 to get ms.
type histRawDTO struct {
	Bucket   time.Time `ch:"bucket"`
	PoolName string    `ch:"pool_name"`
	P50      float64   `ch:"p50"`
	P95      float64   `ch:"p95"`
	P99      float64   `ch:"p99"`
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
		       val_sum / val_count AS value
		FROM observability.metrics_1m
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
		       val_sum / val_count AS value
		FROM observability.metrics_1m
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
		       val_sum / val_count AS value
		FROM observability.metrics_1m
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
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name = @metricName
		)
		SELECT bucket,
		       pool_name,
		       qs[1] AS p50,
		       qs[2] AS p95,
		       qs[3] AS p99
		FROM (
		    SELECT ` + timebucket.DisplayGrainSQL(endMs-startMs) + `                AS bucket,
		           attributes.'pool.name'::String                                    AS pool_name,
		           quantilesPrometheusHistogramMerge(0.5, 0.95, 0.99)(latency_state) AS qs
		    FROM observability.metrics_1m
		    PREWHERE team_id        = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND fingerprint    IN active_fps
		         AND metric_name    = @metricName
		    WHERE timestamp BETWEEN @start AND @end
		    GROUP BY bucket, pool_name
		)
		ORDER BY bucket, pool_name`
	args := append(filter.MetricArgs(teamID, startMs, endMs, metricName), filterArgs...)
	full := query + filterWhere
	var rows []histRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, traceLabel, &rows, full, args...)
}
