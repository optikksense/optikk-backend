package volume

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"

	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetOpsBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error)
	GetOpsByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error)
	GetOpsByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error)
	GetReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ReadWritePoint, error)
	GetOpsByNamespace(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) opsSeriesByAttr(ctx context.Context, teamID int64, startMs, endMs int64, groupAttr string, f shared.Filters) ([]OpsTimeSeries, error) {
	bucket := utils.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	groupExpr := shared.AttrString(groupAttr)

	query := fmt.Sprintf(`
		SELECT
		    %s                        AS time_bucket,
		    %s                        AS group_by,
		    toInt64(sum(hist_count))  AS op_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, groupExpr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)

	var dtos []opsRawDTO
	if err := r.db.Select(ctx, &dtos, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}

	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	out := make([]OpsTimeSeries, len(dtos))
	for i, d := range dtos {
		rate := float64(d.OpCount) / bucketSec
		out[i] = OpsTimeSeries{
			TimeBucket: d.TimeBucket,
			GroupBy:    d.GroupBy,
			OpsPerSec:  &rate,
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetOpsBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBSystem, f)
}

func (r *ClickHouseRepository) GetOpsByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBOperationName, f)
}

func (r *ClickHouseRepository) GetOpsByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBCollectionName, f)
}

func (r *ClickHouseRepository) GetOpsByNamespace(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(ctx, teamID, startMs, endMs, shared.AttrDBNamespace, f)
}

func (r *ClickHouseRepository) GetReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ReadWritePoint, error) {
	bucket := utils.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	opAttr := shared.AttrString(shared.AttrDBOperationName)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                            AS time_bucket,
		    toInt64(sumIf(hist_count, upper(%s) IN ('SELECT', 'FIND', 'GET')))           AS read_count,
		    toInt64(sumIf(hist_count, upper(%s) IN ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'UPSERT', 'SET', 'PUT', 'AGGREGATE'))) AS write_count
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
		opAttr, opAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)

	var dtos []readWriteRawDTO
	if err := r.db.Select(ctx, &dtos, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}

	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	out := make([]ReadWritePoint, len(dtos))
	for i, d := range dtos {
		readRate := float64(d.ReadCount) / bucketSec
		writeRate := float64(d.WriteCount) / bucketSec
		out[i] = ReadWritePoint{
			TimeBucket:     d.TimeBucket,
			ReadOpsPerSec:  &readRate,
			WriteOpsPerSec: &writeRate,
		}
	}
	return out, nil
}
