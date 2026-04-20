package collection

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetCollectionLatency(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]LatencyTimeSeries, error)
	GetCollectionOps(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]OpsTimeSeries, error)
	GetCollectionErrors(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]ErrorTimeSeries, error)
	GetCollectionQueryTexts(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters, limit int) ([]CollectionTopQuery, error)
	GetCollectionReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, collection string) ([]ReadWritePoint, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetCollectionLatency(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]LatencyTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS time_bucket,
		    %s                                                                              AS group_by,
		    quantileTDigestWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p50_ms,
		    quantileTDigestWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p95_ms,
		    quantileTDigestWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @collection
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, shared.AttrString(shared.AttrDBOperationName),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
		fc,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	params = append(params, fargs...)
	var rows []LatencyTimeSeries
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, params...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetCollectionOps(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]OpsTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                               AS time_bucket,
		    %s                               AS group_by,
		    toFloat64(sum(hist_count)) / %f  AS ops_per_sec
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @collection
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, shared.AttrString(shared.AttrDBOperationName),
		bucketSec,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
		fc,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	params = append(params, fargs...)
	var rows []OpsTimeSeries
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, params...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetCollectionErrors(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]ErrorTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                               AS time_bucket,
		    %s                               AS group_by,
		    toFloat64(sum(hist_count)) / %f  AS errors_per_sec
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @collection
		  AND notEmpty(%s)
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, shared.AttrString(shared.AttrErrorType),
		bucketSec,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
		shared.AttrString(shared.AttrErrorType),
		fc,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	params = append(params, fargs...)
	var rows []ErrorTimeSeries
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, params...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetCollectionQueryTexts(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters, limit int) ([]CollectionTopQuery, error) {
	if limit <= 0 {
		limit = 10
	}
	fc, fargs := shared.FilterClauses(f)
	queryAttr := shared.AttrString(shared.AttrDBQueryText)
	errorAttr := shared.AttrString(shared.AttrErrorType)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS query_text,
		    quantileTDigestWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms,
		    toInt64(sum(hist_count))                                                        AS call_count,
		    toInt64(sumIf(hist_count, notEmpty(%s)))                                        AS error_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @collection
		  AND notEmpty(%s)
		  %s
		GROUP BY query_text
		ORDER BY p99_ms DESC
		LIMIT %d
	`,
		queryAttr,
		errorAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
		queryAttr,
		fc, limit,
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	params = append(params, fargs...)
	var rows []CollectionTopQuery
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, params...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetCollectionReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, collection string) ([]ReadWritePoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	opAttr := shared.AttrString(shared.AttrDBOperationName)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                                            AS time_bucket,
		    toFloat64(sumIf(hist_count, upper(%s) IN ('SELECT', 'FIND', 'GET'))) / %f                    AS read_ops_per_sec,
		    toFloat64(sumIf(hist_count, upper(%s) IN ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'UPSERT', 'SET', 'PUT', 'AGGREGATE'))) / %f AS write_ops_per_sec
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = @collection
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		opAttr, bucketSec,
		opAttr, bucketSec,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBCollectionName),
	)

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	var rows []ReadWritePoint
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, params...); err != nil {
		return nil, err
	}
	return rows, nil
}
