package disk

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	GetDiskIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetDiskOperations(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetDiskIOTime(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetFilesystemUsage(teamID int64, startMs, endMs int64) ([]MountpointBucket, error)
	GetFilesystemUtilization(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
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

func (r *ClickHouseRepository) queryDirectionBuckets(query string, teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]DirectionBucket, len(rows))
	for i, row := range rows {
		buckets[i] = DirectionBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Direction: dbutil.StringFromAny(row["direction"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) queryResourceBuckets(query string, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
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

func (r *ClickHouseRepository) GetDiskIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	b := bucket(startMs, endMs)
	dir := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemDiskDirection)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as direction, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, dir, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemDiskIO)
	return r.queryDirectionBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetDiskOperations(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	b := bucket(startMs, endMs)
	dir := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemDiskDirection)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as direction, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, dir, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemDiskOperations)
	return r.queryDirectionBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetDiskIOTime(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	b := bucket(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as pod, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1 ORDER BY 1`,
		b, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemDiskIOTime)
	return r.queryResourceBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetFilesystemUsage(teamID int64, startMs, endMs int64) ([]MountpointBucket, error) {
	b := bucket(startMs, endMs)
	mp := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrFilesystemMountpoint)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as mountpoint, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, mp, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemFilesystemUsage)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]MountpointBucket, len(rows))
	for i, row := range rows {
		buckets[i] = MountpointBucket{
			Timestamp:  dbutil.StringFromAny(row["time_bucket"]),
			Mountpoint: dbutil.StringFromAny(row["mountpoint"]),
			Value:      dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetFilesystemUtilization(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	b := bucket(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as pod, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1 ORDER BY 1`,
		b, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemFilesystemUtil)
	return r.queryResourceBuckets(query, teamID, startMs, endMs)
}
