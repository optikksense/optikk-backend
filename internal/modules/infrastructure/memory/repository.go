package memory

import (
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	GetMemoryUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetMemoryUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetSwapUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) Repository {
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

func (r *ClickHouseRepository) queryStateBuckets(query string, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]StateBucket, len(rows))
	for i, row := range rows {
		buckets[i] = StateBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			State:     dbutil.StringFromAny(row["state"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetMemoryUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemMemoryState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUsage)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetMemoryUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
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
		WHERE %s = ? AND %s BETWEEN ? AND ?
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
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]ResourceBucket, len(rows))
	for i, row := range rows {
		buckets[i] = ResourceBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Pod:       dbutil.StringFromAny(row["pod"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetSwapUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemMemoryState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemPagingUsage)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}
