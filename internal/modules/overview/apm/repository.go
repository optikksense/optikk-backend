package apm

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

type Repository interface {
	GetRPCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetRPCRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketDTO, error)
	GetMessagingPublishDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetProcessCPU(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetProcessMemory(ctx context.Context, teamID int64, startMs, endMs int64) (processMemoryDTO, error)
	GetOpenFDs(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketDTO, error)
	GetUptime(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketDTO, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) queryHistogramSummary(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (histogramSummaryDTO, error) {
	query := fmt.Sprintf(`
		SELECT
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99,
		    avg(hist_sum / nullIf(hist_count, 0))                                     AS avg
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
	`,
		TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, metricName,
	)
	var result histogramSummaryDTO
	return result, r.db.QueryRow(ctx, &result, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
}

func (r *ClickHouseRepository) queryTimeBuckets(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]timeBucketDTO, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s         AS time_bucket,
		    avg(value) AS val
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
	var rows []timeBucketDTO
	return rows, r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
}

func (r *ClickHouseRepository) GetRPCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricRPCServerDuration)
}

func (r *ClickHouseRepository) GetRPCRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketDTO, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s               AS time_bucket,
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
	var rows []timeBucketDTO
	return rows, r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
}

func (r *ClickHouseRepository) GetMessagingPublishDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricMessagingPublishDuration)
}

func (r *ClickHouseRepository) GetProcessCPU(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	bucket := timebucket.Expression(startMs, endMs)
	stateAttr := attrString(AttrProcessCPUState)

	query := fmt.Sprintf(`
		SELECT
		    %s         AS time_bucket,
		    %s         AS state,
		    avg(value) AS val
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
	var rows []StateBucket
	return rows, r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
}

func (r *ClickHouseRepository) GetProcessMemory(ctx context.Context, teamID int64, startMs, endMs int64) (processMemoryDTO, error) {
	query := fmt.Sprintf(`
		SELECT
		    avgIf(value, %s = '%s') AS rss,
		    avgIf(value, %s = '%s') AS vms
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')
	`,
		ColMetricName, MetricProcessMemoryUsage,
		ColMetricName, MetricProcessMemoryVirtual,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricProcessMemoryUsage, MetricProcessMemoryVirtual,
	)
	var result processMemoryDTO
	return result, r.db.QueryRow(ctx, &result, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
}

func (r *ClickHouseRepository) GetOpenFDs(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketDTO, error) {
	return r.queryTimeBuckets(ctx, teamID, startMs, endMs, MetricProcessOpenFDs)
}

func (r *ClickHouseRepository) GetUptime(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketDTO, error) {
	return r.queryTimeBuckets(ctx, teamID, startMs, endMs, MetricProcessUptime)
}
