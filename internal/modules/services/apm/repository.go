package apm

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

type HistogramAggRow struct {
	SumHistSum   float64   `ch:"sum_hist_sum"`
	SumHistCount uint64    `ch:"sum_hist_count"`
	Buckets      []float64 `ch:"hist_buckets"`
	Counts       []uint64  `ch:"hist_counts"`
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
	QueryHistogramAgg(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (HistogramAggRow, error)
	QueryMetricSeries(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]MetricSeriesRow, error)
	QueryStateSeries(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]StateSeriesRow, error)
	QueryGaugeAvgByName(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]NamedAvgRow, error)
	QueryHistogramCountSeries(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]CountSeriesRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) QueryHistogramAgg(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (HistogramAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    sum(hist_sum)            AS sum_hist_sum,
		    sum(hist_count)          AS sum_hist_count,
		    max(hist_buckets)        AS hist_buckets,
		    sumForEach(hist_counts)  AS hist_counts
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end`
	var row HistogramAggRow
	err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryHistogramAgg",
		&row, query, singleMetricArgs(teamID, startMs, endMs, metricName)...)
	return row, err
}

func (r *ClickHouseRepository) QueryMetricSeries(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]MetricSeriesRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT timestamp, val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		ORDER BY timestamp`
	var rows []MetricSeriesRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryMetricSeries",
		&rows, query, singleMetricArgs(teamID, startMs, endMs, metricName)...)
	return rows, err
}

func (r *ClickHouseRepository) QueryStateSeries(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]StateSeriesRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    timestamp,
		    attributes.` + "`process.cpu.state`" + `::String AS state,
		    value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		ORDER BY timestamp, state`
	var rows []StateSeriesRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryStateSeries",
		&rows, query, singleMetricArgs(teamID, startMs, endMs, metricName)...)
	return rows, err
}

func (r *ClickHouseRepository) QueryGaugeAvgByName(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]NamedAvgRow, error) {
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
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryGaugeAvgByName",
		&rows, query, multiMetricArgs(teamID, startMs, endMs, metricNames)...)
	return rows, err
}

func (r *ClickHouseRepository) QueryHistogramCountSeries(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]CountSeriesRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT timestamp, hist_count AS count
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		ORDER BY timestamp`
	var rows []CountSeriesRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "apm.QueryHistogramCountSeries",
		&rows, query, singleMetricArgs(teamID, startMs, endMs, metricName)...)
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
