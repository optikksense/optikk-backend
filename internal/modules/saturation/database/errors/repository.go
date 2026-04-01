package errors

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"

	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetErrorsBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error)
	GetErrorsByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error)
	GetErrorsByErrorType(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error)
	GetErrorsByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error)
	GetErrorsByResponseStatus(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error)
	GetErrorRatio(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorRatioPoint, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) errorSeriesByAttr(ctx context.Context, teamID int64, startMs, endMs int64, groupAttr string, f shared.Filters) ([]ErrorTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	groupExpr := shared.AttrString(groupAttr)
	errorAttr := shared.AttrString(shared.AttrErrorType)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                        AS time_bucket,
		    %s                                                        AS group_by,
		    toFloat64(sum(hist_count)) / %f                          AS errors_per_sec
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, groupExpr,
		bucketSec,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		errorAttr,
		fc,
	)

	var rows []ErrorTimeSeries
	if err := r.db.Select(ctx, &rows, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetErrorsBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error) {
	return r.errorSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBSystem, f)
}

func (r *ClickHouseRepository) GetErrorsByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error) {
	return r.errorSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBOperationName, f)
}

func (r *ClickHouseRepository) GetErrorsByErrorType(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error) {
	return r.errorSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrErrorType, f)
}

func (r *ClickHouseRepository) GetErrorsByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error) {
	return r.errorSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBCollectionName, f)
}

func (r *ClickHouseRepository) GetErrorsByResponseStatus(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error) {
	return r.errorSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBResponseStatus, f)
}

func (r *ClickHouseRepository) GetErrorRatio(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorRatioPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	errorAttr := shared.AttrString(shared.AttrErrorType)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS time_bucket,
		    toFloat64(sumIf(hist_count, notEmpty(%s))) / nullIf(toFloat64(sum(hist_count)), 0) * 100 AS error_ratio_pct
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		errorAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)

	var rows []ErrorRatioPoint
	if err := r.db.Select(ctx, &rows, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}
	return rows, nil
}
