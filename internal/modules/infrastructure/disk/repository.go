package disk

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	GetDiskIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error)
	GetDiskOperations(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error)
	GetDiskIOTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetFilesystemUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]mountpointBucketDTO, error)
	GetFilesystemUtilization(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
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

func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func (r *ClickHouseRepository) queryDirectionBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	var rows []directionBucketDTO
	if err := r.db.Select(ctx, &rows, query, baseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) queryResourceBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	var rows []resourceBucketDTO
	if err := r.db.Select(ctx, &rows, query, baseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetDiskIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	b := bucket(startMs, endMs)
	dir := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemDiskDirection)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as direction, sum(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, dir, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemDiskIO)
	return r.queryDirectionBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetDiskOperations(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	b := bucket(startMs, endMs)
	dir := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemDiskDirection)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as direction, sum(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, dir, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemDiskOperations)
	return r.queryDirectionBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetDiskIOTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	b := bucket(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as pod, sum(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1 ORDER BY 1`,
		b, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemDiskIOTime)
	return r.queryResourceBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetFilesystemUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]mountpointBucketDTO, error) {
	b := bucket(startMs, endMs)
	mp := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrFilesystemMountpoint)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as mountpoint, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, mp, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemFilesystemUsage)
	var rows []mountpointBucketDTO
	if err := r.db.Select(ctx, &rows, query, baseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetFilesystemUtilization(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	b := bucket(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as pod, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1 ORDER BY 1`,
		b, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemFilesystemUtil)
	return r.queryResourceBuckets(ctx, query, teamID, startMs, endMs)
}
