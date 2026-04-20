package apm

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

type Repository interface {
	GetRPCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetRPCRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error)
	GetMessagingPublishDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetProcessCPU(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketRow, error)
	GetProcessMemory(ctx context.Context, teamID int64, startMs, endMs int64) (processMemoryRow, error)
	GetOpenFDs(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketRow, error)
	GetUptime(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

// histogramSummaryRow is the sum/count carrier for histogram-backed metrics.
// Percentiles come from sketch.Querier; avg is computed from hist_sum/hist_count.
type histogramSummaryRow struct {
	HistSum   float64 `ch:"hist_sum"`
	HistCount int64   `ch:"hist_count"`
	// Placeholder slots preserved for parity with the old schema; service
	// overwrites them from the matching sketch kind.
	P50 float64 `ch:"p50"`
	P95 float64 `ch:"p95"`
	P99 float64 `ch:"p99"`
}

// timeBucketRow carries per-bucket sum/count so the service can compute avg.
type timeBucketRow struct {
	Timestamp string  `ch:"time_bucket"`
	ValSum    float64 `ch:"val_sum"`
	ValCount  int64   `ch:"val_count"`
}

// stateBucketRow carries per-(bucket,state) sum/count for the Go-side avg.
type stateBucketRow struct {
	Timestamp string  `ch:"time_bucket"`
	State     string  `ch:"state"`
	ValSum    float64 `ch:"val_sum"`
	ValCount  int64   `ch:"val_count"`
}

// processMemoryRow carries the sum/count pair per memory kind (RSS, VMS).
type processMemoryRow struct {
	RSSSum   float64 `ch:"rss_sum"`
	RSSCount int64   `ch:"rss_count"`
	VMSSum   float64 `ch:"vms_sum"`
	VMSCount int64   `ch:"vms_count"`
}

func (r *ClickHouseRepository) queryHistogramSummary(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (histogramSummaryRow, error) {
	// Quantiles come from sketch.Querier in service.go; CH just returns totals
	// so Go can compute avg = hist_sum / hist_count.
	query := fmt.Sprintf(`
		SELECT
		    sum(hist_sum)                AS hist_sum,
		    toInt64(sum(hist_count))     AS hist_count,
		    0 AS p50, 0 AS p95, 0 AS p99
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
	`,
		TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, metricName,
	)
	var row histogramSummaryRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...).ScanStruct(&row); err != nil {
		return histogramSummaryRow{}, err
	}
	return row, nil
}

func (r *ClickHouseRepository) queryTimeBucketsSumCount(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]timeBucketRow, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                        AS time_bucket,
		    sum(value)                AS val_sum,
		    toInt64(count())          AS val_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, metricName,
	)
	var rows []timeBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRPCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricRPCServerDuration)
}

func (r *ClickHouseRepository) GetRPCRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                 AS time_bucket,
		    toFloat64(count()) AS val
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricRPCServerDuration,
	)
	var rows []TimeBucket
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetMessagingPublishDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricMessagingPublishDuration)
}

func (r *ClickHouseRepository) GetProcessCPU(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketRow, error) {
	bucket := timebucket.Expression(startMs, endMs)
	stateAttr := attrString(AttrProcessCPUState)

	query := fmt.Sprintf(`
		SELECT
		    %s               AS time_bucket,
		    %s               AS state,
		    sum(value)       AS val_sum,
		    toInt64(count()) AS val_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		GROUP BY time_bucket, state
		ORDER BY time_bucket, state
	`,
		bucket, stateAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricProcessCPUTime,
	)
	var rows []stateBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetProcessMemory(ctx context.Context, teamID int64, startMs, endMs int64) (processMemoryRow, error) {
	query := fmt.Sprintf(`
		SELECT
		    sumIf(value, %s = '%s')              AS rss_sum,
		    toInt64(countIf(%s = '%s'))          AS rss_count,
		    sumIf(value, %s = '%s')              AS vms_sum,
		    toInt64(countIf(%s = '%s'))          AS vms_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')
	`,
		ColMetricName, MetricProcessMemoryUsage,
		ColMetricName, MetricProcessMemoryUsage,
		ColMetricName, MetricProcessMemoryVirtual,
		ColMetricName, MetricProcessMemoryVirtual,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricProcessMemoryUsage, MetricProcessMemoryVirtual,
	)
	var row processMemoryRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...).ScanStruct(&row); err != nil {
		return processMemoryRow{}, err
	}
	return row, nil
}

func (r *ClickHouseRepository) GetOpenFDs(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketRow, error) {
	return r.queryTimeBucketsSumCount(ctx, teamID, startMs, endMs, MetricProcessOpenFDs)
}

func (r *ClickHouseRepository) GetUptime(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketRow, error) {
	return r.queryTimeBucketsSumCount(ctx, teamID, startMs, endMs, MetricProcessUptime)
}
