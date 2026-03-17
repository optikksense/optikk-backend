package cpu

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	GetCPUTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetLoadAverage(ctx context.Context, teamID int64, startMs, endMs int64) (loadAverageResultDTO, error)
	GetProcessCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) Repository {
	return &ClickHouseRepository{db: db}
}

func bucket(startMs, endMs int64) string {
	return infraconsts.TimeBucketExpression(startMs, endMs)
}

func syncAverageExpr(parts ...string) string {
	joined := strings.Join(parts, ", ")
	return `if(
		length(arrayFilter(x -> isNotNull(x), [` + joined + `])) > 0,
		arrayReduce('avg', arrayFilter(x -> isNotNull(x), [` + joined + `])),
		NULL
	)`
}

func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func (r *ClickHouseRepository) queryStateBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	var rows []stateBucketDTO
	if err := r.db.Select(ctx, &rows, query, baseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetCPUTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemCPUState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, sum(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUTime)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	b := bucket(startMs, endMs)
	aCPU := infraconsts.AttrFloat(infraconsts.AttrSystemCPUUtilization)

	cpuSystemCol := fmt.Sprintf(`if(countIf(%s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %.1f) > 0, avgIf(%s * %.1f, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %.1f), NULL)`,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage,
		infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold,
		infraconsts.ColValue, infraconsts.PercentageMultiplier,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage,
		infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold)
	cpuProcessCol := fmt.Sprintf(`if(countIf(%s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %.1f) > 0, avgIf(%s * %.1f, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %.1f), NULL)`,
		infraconsts.ColMetricName, infraconsts.MetricProcessCPUUsage,
		infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold,
		infraconsts.ColValue, infraconsts.PercentageMultiplier,
		infraconsts.ColMetricName, infraconsts.MetricProcessCPUUsage,
		infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold)
	cpuAttrCol := fmt.Sprintf(`if(countIf(%s >= 0 AND %s <= %.1f) > 0, avgIf(%s * %.1f, %s >= 0 AND %s <= %.1f), NULL)`,
		aCPU, aCPU, infraconsts.PercentageThreshold,
		aCPU, infraconsts.PercentageMultiplier, aCPU, aCPU, infraconsts.PercentageThreshold)
	cpuCol := syncAverageExpr(cpuSystemCol, cpuProcessCol, cpuAttrCol)

	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       %s as pod,
		       %s as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )
		GROUP BY 1, 2
		HAVING pod != ''
		ORDER BY 1 ASC, 2 ASC
	`, b, infraconsts.ColServiceName, cpuCol,
		infraconsts.TableMetrics, infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.MetricProcessCPUUsage,
		aCPU)
	var rows []resourceBucketDTO
	if err := r.db.Select(ctx, &rows, query, baseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetLoadAverage(ctx context.Context, teamID int64, startMs, endMs int64) (loadAverageResultDTO, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s, %s = '%s') as load_1m,
			avgIf(%s, %s = '%s') as load_5m,
			avgIf(%s, %s = '%s') as load_15m
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s', '%s')`,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg1m,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg5m,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg15m,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg1m, infraconsts.MetricSystemCPULoadAvg5m, infraconsts.MetricSystemCPULoadAvg15m)
	var result loadAverageResultDTO
	if err := r.db.QueryRow(ctx, &result, query, baseParams(teamID, startMs, endMs)...); err != nil {
		return loadAverageResultDTO{}, err
	}
	return result, nil
}

func (r *ClickHouseRepository) GetProcessCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	status := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrProcessStatus)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, status, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemProcessCount)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}
