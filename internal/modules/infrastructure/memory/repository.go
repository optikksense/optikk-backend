package memory

import (
	"context"
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	GetMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetSwapUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) Repository {
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

func (r *ClickHouseRepository) queryStateBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	var rows []stateBucketDTO
	if err := r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemMemoryState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUsage)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	b := bucket(startMs, endMs)
	aMem := infraconsts.AttrFloat(infraconsts.AttrSystemMemoryUtilization)

	memSystemCol := fmt.Sprintf(`if(countIf(%s = '%s' AND isFinite(%s)) > 0, avgIf(if(%s <= %.1f, %s * %.1f, %s), %s = '%s' AND isFinite(%s)), NULL)`,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.PercentageThreshold, infraconsts.ColValue, infraconsts.PercentageMultiplier, infraconsts.ColValue,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.ColValue)
	memJvmCol := fmt.Sprintf(`if(sumIf(%s, %s = '%s' AND %s > 0 AND isFinite(%s)) > 0, %.1f * sumIf(%s, %s = '%s' AND %s >= 0 AND isFinite(%s)) / nullIf(sumIf(%s, %s = '%s' AND %s > 0 AND isFinite(%s)), 0), NULL)`,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryMax, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageMultiplier,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed, infraconsts.ColValue, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryMax, infraconsts.ColValue, infraconsts.ColValue)
	memAttrCol := fmt.Sprintf(`if(countIf(%s > 0) > 0, avgIf(if(%s <= %.1f, %s * %.1f, %s), %s > 0), NULL)`,
		aMem, aMem, infraconsts.PercentageThreshold,
		aMem, infraconsts.PercentageMultiplier, aMem, aMem)
	memCol := syncAverageExpr(memSystemCol, memJvmCol, memAttrCol)

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
	`, b, infraconsts.ColServiceName, memCol,
		infraconsts.TableMetrics, infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.MetricJVMMemoryUsed, infraconsts.MetricJVMMemoryMax,
		aMem)
	var rows []resourceBucketDTO
	if err := r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSwapUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemMemoryState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemPagingUsage)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}
