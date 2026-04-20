package errors

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/sync/errgroup"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"

	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
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
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// errorRateRawDTO is the per-(time_bucket, group_by) histogram count leg.
// Service layer divides by bucket width to yield errors_per_sec.
type errorRateRawDTO struct {
	TimeBucket string `ch:"time_bucket"`
	GroupBy    string `ch:"group_by"`
	N          uint64 `ch:"n"`
}

func (r *ClickHouseRepository) errorSeriesByAttr(ctx context.Context, teamID int64, startMs, endMs int64, groupAttr string, f shared.Filters) ([]ErrorTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	groupExpr := shared.AttrString(groupAttr)
	errorAttr := shared.AttrString(shared.AttrErrorType)

	query := fmt.Sprintf(`
		SELECT
		    %s              AS time_bucket,
		    %s              AS group_by,
		    sum(hist_count) AS n
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
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		errorAttr,
		fc,
	)

	var dtos []errorRateRawDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}

	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	out := make([]ErrorTimeSeries, len(dtos))
	for i, d := range dtos {
		rate := float64(d.N) / bucketSec
		out[i] = ErrorTimeSeries{
			TimeBucket:   d.TimeBucket,
			GroupBy:      d.GroupBy,
			ErrorsPerSec: &rate,
		}
	}
	return out, nil
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

// errorRatioLegDTO is the per-bucket hist_count leg (totals or errors) used
// to merge the error ratio in Go.
type errorRatioLegDTO struct {
	TimeBucket string `ch:"time_bucket"`
	N          uint64 `ch:"n"`
}

// GetErrorRatio runs two parallel scans — totals and errors (adds `AND
// notEmpty(error_type)` to WHERE) — and divides in Go. Replaces the prior
// `sumIf(...) / nullIf(sum(...), 0) * 100` SELECT expression.
func (r *ClickHouseRepository) GetErrorRatio(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorRatioPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	errorAttr := shared.AttrString(shared.AttrErrorType)
	params := append(shared.BaseParams(teamID, startMs, endMs), fargs...)

	totalsQuery := fmt.Sprintf(`
		SELECT
		    %s              AS time_bucket,
		    sum(hist_count) AS n
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
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)

	errorsQuery := fmt.Sprintf(`
		SELECT
		    %s              AS time_bucket,
		    sum(hist_count) AS n
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		errorAttr,
		fc,
	)

	var (
		totals []errorRatioLegDTO
		errs   []errorRatioLegDTO
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, totalsQuery, params...)
	})
	g.Go(func() error {
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, errorsQuery, params...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[e.TimeBucket] = e.N
	}

	out := make([]ErrorRatioPoint, 0, len(totals))
	for _, t := range totals {
		var ratio *float64
		if t.N > 0 {
			v := float64(errIdx[t.TimeBucket]) / float64(t.N) * 100
			ratio = &v
		}
		out = append(out, ErrorRatioPoint{
			TimeBucket:    t.TimeBucket,
			ErrorRatioPct: ratio,
		})
	}
	return out, nil
}
