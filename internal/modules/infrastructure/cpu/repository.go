package cpu

import (
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	GetCPUTime(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetCPUUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetLoadAverage(teamID int64, startMs, endMs int64) (LoadAverageResult, error)
	GetProcessCount(teamID int64, startMs, endMs int64) ([]StateBucket, error)
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

func (r *ClickHouseRepository) GetCPUTime(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemCPUState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUTime)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetCPUUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
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
		WHERE %s = ? AND %s BETWEEN ? AND ?
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

func (r *ClickHouseRepository) GetLoadAverage(teamID int64, startMs, endMs int64) (LoadAverageResult, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s, %s = '%s') as load_1m,
			avgIf(%s, %s = '%s') as load_5m,
			avgIf(%s, %s = '%s') as load_15m
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s', '%s')`,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg1m,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg5m,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg15m,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg1m, infraconsts.MetricSystemCPULoadAvg5m, infraconsts.MetricSystemCPULoadAvg15m)
	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return LoadAverageResult{}, err
	}
	return LoadAverageResult{
		Load1m:  dbutil.Float64FromAny(row["load_1m"]),
		Load5m:  dbutil.Float64FromAny(row["load_5m"]),
		Load15m: dbutil.Float64FromAny(row["load_15m"]),
	}, nil
}

func (r *ClickHouseRepository) GetProcessCount(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	b := bucket(startMs, endMs)
	status := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrProcessStatus)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, status, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemProcessCount)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}
