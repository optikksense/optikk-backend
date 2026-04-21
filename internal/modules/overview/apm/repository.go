package apm

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const metricsHistRollupPrefix = "observability.metrics_histograms_rollup"

// queryIntervalMinutes returns the group-by step (in minutes) for rollup
// reads. It is max(tierStep, dashboardStep) so the step is never finer than
// the selected tier's native resolution.
func queryIntervalMinutes(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var dashStep int64
	switch {
	case hours <= 3:
		dashStep = 1
	case hours <= 24:
		dashStep = 5
	case hours <= 168:
		dashStep = 60
	default:
		dashStep = 1440
	}
	if tierStepMin > dashStep {
		return tierStepMin
	}
	return dashStep
}

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
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

// histogramSummaryRawRow scans merged sketch-tuple output plus the
// duration-sum / count state columns; the service's `avg` is derived
// Go-side from sum/count.
type histogramSummaryRawRow struct {
	P50     float64 `ch:"p50"`
	P95     float64 `ch:"p95"`
	P99     float64 `ch:"p99"`
	HistSum float64 `ch:"hist_sum"`
	HistCnt uint64  `ch:"hist_count"`
}

func (r *ClickHouseRepository) queryHistogramSummary(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (histogramSummaryDTO, error) {
	table, _ := rollup.TierTableFor(metricsHistRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 AS p50,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 AS p99,
		    sumMerge(hist_sum)                                                  AS hist_sum,
		    sumMerge(hist_count)                                                AS hist_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName`, table)

	var raw histogramSummaryRawRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, rollupParams(teamID, startMs, endMs, metricName)...).ScanStruct(&raw)
	if err != nil {
		return histogramSummaryDTO{}, err
	}
	avg := 0.0
	if raw.HistCnt > 0 {
		avg = raw.HistSum / float64(raw.HistCnt)
	}
	return histogramSummaryDTO{P50: raw.P50, P95: raw.P95, P99: raw.P99, Avg: avg}, nil
}

func rollupParams(teamID int64, startMs, endMs int64, metricName string) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", metricName),
	}
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
	return rows, r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
}

func (r *ClickHouseRepository) GetRPCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricRPCServerDuration)
}

func (r *ClickHouseRepository) GetRPCRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketDTO, error) {
	// Pre-aggregated call count from the histogram rollup's `hist_count` state.
	table, tierStep := rollup.TierTableFor(metricsHistRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       sumMerge(hist_count) AS val_u64
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		GROUP BY time_bucket
		ORDER BY time_bucket`, table)
	args := append(rollupParams(teamID, startMs, endMs, MetricRPCServerDuration),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	)

	var raw []struct {
		Timestamp time.Time `ch:"time_bucket"`
		ValU64    uint64    `ch:"val_u64"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]timeBucketDTO, len(raw))
	for i, row := range raw {
		val := float64(row.ValU64)
		rows[i] = timeBucketDTO{Timestamp: row.Timestamp.UTC().Format("2006-01-02 15:04:05"), Value: &val}
	}
	return rows, nil
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
	return rows, r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
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
	return result, r.db.QueryRow(dbutil.OverviewCtx(ctx), query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...).ScanStruct(&result)
}

func (r *ClickHouseRepository) GetOpenFDs(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketDTO, error) {
	return r.queryTimeBuckets(ctx, teamID, startMs, endMs, MetricProcessOpenFDs)
}

func (r *ClickHouseRepository) GetUptime(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketDTO, error) {
	return r.queryTimeBuckets(ctx, teamID, startMs, endMs, MetricProcessUptime)
}
