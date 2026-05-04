package apm

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

type HistogramAggRow struct {
	SumHistSum   float64 `ch:"sum_hist_sum"`
	SumHistCount uint64  `ch:"sum_hist_count"`
	P50          float64 `ch:"p50"`
	P95          float64 `ch:"p95"`
	P99          float64 `ch:"p99"`
}

type MetricSeriesRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Value     float64   `ch:"value"`
}

type StateSeriesRow struct {
	Timestamp time.Time `ch:"timestamp"`
	State     string    `ch:"state"`
	Value     float64   `ch:"value"`
}

type NamedAvgRow struct {
	MetricName string  `ch:"metric_name"`
	Avg        float64 `ch:"avg"`
}

type CountSeriesRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Count     uint64    `ch:"count"`
}

type Repository interface {
	QueryRPCDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error)
	QueryMessagingPublishDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error)
	QueryRPCRequestCountSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]CountSeriesRow, error)
	QueryProcessCPUStateSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateSeriesRow, error)
	QueryProcessMemoryAvg(ctx context.Context, teamID int64, startMs, endMs int64) ([]NamedAvgRow, error)
	QueryProcessOpenFDsSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]MetricSeriesRow, error)
	QueryProcessUptimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]MetricSeriesRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

const histogramAggQuery = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT sum_hist_sum,
		       sum_hist_count,
		       qs[1] AS p50,
		       qs[2] AS p95,
		       qs[3] AS p99
		FROM (
		    SELECT sum(hist_sum)                                                  AS sum_hist_sum,
		           sum(hist_count)                                                AS sum_hist_count,
		           quantilesPrometheusHistogramMerge(0.5, 0.95, 0.99)(latency_state) AS qs
		    FROM observability.metrics_1m
		    PREWHERE team_id        = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND fingerprint   IN active_fps
		    WHERE metric_name = @metricName
		      AND timestamp BETWEEN @start AND @end
		)`

func (r *ClickHouseRepository) QueryRPCDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	var row HistogramAggRow
	err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryRPCDurationHistogram",
		&row, histogramAggQuery, singleMetricArgs(teamID, startMs, endMs, MetricRPCServerDuration)...)
	return row, err
}

func (r *ClickHouseRepository) QueryMessagingPublishDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	var row HistogramAggRow
	err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryMessagingPublishDurationHistogram",
		&row, histogramAggQuery, singleMetricArgs(teamID, startMs, endMs, MetricMessagingPublishDuration)...)
	return row, err
}

func (r *ClickHouseRepository) QueryRPCRequestCountSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]CountSeriesRow, error) {
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT ` + timebucket.DisplayGrainSQL(endMs-startMs) + ` AS timestamp,
		       sum(hist_count)                                  AS count
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		GROUP BY timestamp
		ORDER BY timestamp`
	var rows []CountSeriesRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryRPCRequestCountSeries",
		&rows, query, singleMetricArgs(teamID, startMs, endMs, MetricRPCServerDuration)...)
	return rows, err
}

func (r *ClickHouseRepository) QueryProcessCPUStateSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateSeriesRow, error) {
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    ` + timebucket.DisplayGrainSQL(endMs-startMs) + ` AS timestamp,
		    attributes.` + "`process.cpu.state`" + `::String AS state,
		    avg(value)                                       AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		GROUP BY timestamp, state
		ORDER BY timestamp, state`
	var rows []StateSeriesRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryProcessCPUStateSeries",
		&rows, query, singleMetricArgs(teamID, startMs, endMs, MetricProcessCPUTime)...)
	return rows, err
}

func (r *ClickHouseRepository) QueryProcessMemoryAvg(ctx context.Context, teamID int64, startMs, endMs int64) ([]NamedAvgRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT metric_name, sum(val_sum) / sum(val_count) AS avg
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		GROUP BY metric_name`
	var rows []NamedAvgRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryProcessMemoryAvg",
		&rows, query, multiMetricArgs(teamID, startMs, endMs, []string{MetricProcessMemoryUsage, MetricProcessMemoryVirtual})...)
	return rows, err
}

// metricSeriesQuerySQL emits a per-display-bucket gauge series. avg() over
// the underlying 1-min `val_sum/val_count` per-row rate gives mean-of-means
// across raw minutes within each display bucket.
func metricSeriesQuerySQL(windowMs int64) string {
	return `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT ` + timebucket.DisplayGrainSQL(windowMs) + ` AS timestamp,
		       avg(val_sum / val_count)                    AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		GROUP BY timestamp
		ORDER BY timestamp`
}

func (r *ClickHouseRepository) QueryProcessOpenFDsSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]MetricSeriesRow, error) {
	var rows []MetricSeriesRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryProcessOpenFDsSeries",
		&rows, metricSeriesQuerySQL(endMs-startMs), singleMetricArgs(teamID, startMs, endMs, MetricProcessOpenFDs)...)
	return rows, err
}

func (r *ClickHouseRepository) QueryProcessUptimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]MetricSeriesRow, error) {
	var rows []MetricSeriesRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryProcessUptimeSeries",
		&rows, metricSeriesQuerySQL(endMs-startMs), singleMetricArgs(teamID, startMs, endMs, MetricProcessUptime)...)
	return rows, err
}

func singleMetricArgs(teamID int64, startMs, endMs int64, metricName string) []any {
	bucketStart, bucketEnd := bucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — TeamID fits UInt32
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", metricName),
	}
}

func multiMetricArgs(teamID int64, startMs, endMs int64, metricNames []string) []any {
	bucketStart, bucketEnd := bucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — TeamID fits UInt32
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", metricNames),
	}
}

func bucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
