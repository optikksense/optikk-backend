package volume

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
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
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) opsSeriesByAttr(ctx context.Context, teamID int64, startMs, endMs int64, groupAttr string, f shared.Filters) ([]OpsTimeSeries, error) {
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	fc, fargs := shared.RollupFilterClauses(f)
	groupCol := shared.GroupColumnFor(groupAttr)
	if groupCol == "" {
		groupCol = "db_operation"
	}

	query := fmt.Sprintf(`
		SELECT
		    %s                             AS time_bucket,
		    %s                             AS group_by,
		    toInt64(sum(hist_count))  AS op_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, groupCol, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)

	var dtos []opsRawDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "volume.opsSeriesByAttr", &dtos, query, args...); err != nil {
		return nil, err
	}

	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	out := make([]OpsTimeSeries, len(dtos))
	for i, d := range dtos {
		rate := float64(d.OpCount) / bucketSec
		out[i] = OpsTimeSeries{
			TimeBucket:	d.TimeBucket,
			GroupBy:	d.GroupBy,
			OpsPerSec:	&rate,
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
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	fc, fargs := shared.RollupFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                                     AS time_bucket,
		    toInt64(sumMergeIf(hist_count, upper(db_operation) IN ('SELECT', 'FIND', 'GET')))     AS read_count,
		    toInt64(sumMergeIf(hist_count, upper(db_operation) IN ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'UPSERT', 'SET', 'PUT', 'AGGREGATE'))) AS write_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket
		ORDER BY time_bucket
	`, shared.BucketTimeExpr, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)

	var dtos []readWriteRawDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "volume.GetReadVsWrite", &dtos, query, args...); err != nil {
		return nil, err
	}

	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	out := make([]ReadWritePoint, len(dtos))
	for i, d := range dtos {
		readRate := float64(d.ReadCount) / bucketSec
		writeRate := float64(d.WriteCount) / bucketSec
		out[i] = ReadWritePoint{
			TimeBucket:	d.TimeBucket,
			ReadOpsPerSec:	&readRate,
			WriteOpsPerSec:	&writeRate,
		}
	}
	return out, nil
}
