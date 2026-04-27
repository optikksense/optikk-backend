package errors

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
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

type errorRawRow struct {
	TimeBucket	string	`ch:"time_bucket"`
	GroupBy		string	`ch:"group_by"`
	ErrCount	uint64	`ch:"err_count"`
}

func (r *ClickHouseRepository) errorSeriesByAttr(ctx context.Context, teamID int64, startMs, endMs int64, groupAttr string, f shared.Filters) ([]ErrorTimeSeries, error) {
	table := "observability.spans"
	tierStep := int64(1)
	fc, fargs := shared.RollupFilterClauses(f)
	groupCol := shared.GroupColumnFor(groupAttr)

	query := fmt.Sprintf(`
		SELECT
		    %s                                AS time_bucket,
		    %s                                AS group_by,
		    sum(hist_count)              AS err_count
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND error_type != ''
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, groupCol, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)

	var raw []errorRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.errorSeriesByAttr", &raw, query, args...); err != nil {
		return nil, err
	}

	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	out := make([]ErrorTimeSeries, len(raw))
	for i, row := range raw {
		rate := float64(row.ErrCount) / bucketSec	//nolint:gosec // domain-bounded hist_count
		out[i] = ErrorTimeSeries{
			TimeBucket:	row.TimeBucket,
			GroupBy:	row.GroupBy,
			ErrorsPerSec:	&rate,
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

// GetErrorsByResponseStatus groups by `db_response_status_code` on db_histograms_rollup.
func (r *ClickHouseRepository) GetErrorsByResponseStatus(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error) {
	return r.errorSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBResponseStatus, f)
}

type errorRatioRawRow struct {
	TimeBucket	string	`ch:"time_bucket"`
	ErrCount	uint64	`ch:"err_count"`
	TotalCount	uint64	`ch:"total_count"`
}

func (r *ClickHouseRepository) GetErrorRatio(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorRatioPoint, error) {
	table := "observability.spans"
	tierStep := int64(1)
	fc, fargs := shared.RollupFilterClauses(f)

	// Sub-aggregate so we can compute errored-vs-total at bucket level from
	// the single rollup table. `sumIf` on merged counts is not directly
	// supported; instead aggregate per (bucket, error_type) then fold in Go.
	query := fmt.Sprintf(`
		SELECT
		    time_bucket,
		    sumIf(hc, err_flag)        AS err_count,
		    sum(hc)                    AS total_count
		FROM (
		    SELECT
		        %s                                                   AS time_bucket,
		        error_type != ''                                     AS err_flag,
		        sum(hist_count)                                 AS hc
		    FROM %s
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @start AND @end
		      AND metric_name = @metricName
		      %s
		    GROUP BY time_bucket, err_flag
		)
		GROUP BY time_bucket
		ORDER BY time_bucket
	`, shared.BucketTimeExpr, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)

	var raw []errorRatioRawRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetErrorRatio", &raw, query, args...); err != nil {
		return nil, err
	}

	out := make([]ErrorRatioPoint, len(raw))
	for i, row := range raw {
		var pct *float64
		if row.TotalCount > 0 {
			p := float64(row.ErrCount) / float64(row.TotalCount) * 100.0	//nolint:gosec // domain-bounded hist_count
			pct = &p
		}
		out[i] = ErrorRatioPoint{
			TimeBucket:	row.TimeBucket,
			ErrorRatioPct:	pct,
		}
	}
	return out, nil
}
