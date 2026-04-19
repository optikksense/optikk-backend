package slowqueries

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetSlowQueryPatterns(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]SlowQueryPattern, error)
	GetSlowestCollections(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]SlowCollectionRow, error)
	GetSlowQueryRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, thresholdMs float64) ([]SlowRatePoint, error)
	GetP99ByQueryText(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]P99ByQueryText, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSlowQueryPatterns(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]SlowQueryPattern, error) {
	if limit <= 0 {
		limit = 10
	}
	fc, fargs := shared.FilterClauses(f)
	queryAttr := shared.AttrString(shared.AttrDBQueryText)
	collAttr := shared.AttrString(shared.AttrDBCollectionName)
	errorAttr := shared.AttrString(shared.AttrErrorType)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS query_text,
		    %s                                                                              AS collection_name,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p50_ms,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p95_ms,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms,
		    toInt64(sum(hist_count))                                                        AS call_count,
		    toInt64(sumIf(hist_count, notEmpty(%s)))                                        AS error_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
		GROUP BY query_text, collection_name
		ORDER BY p99_ms DESC
		LIMIT %d
	`,
		queryAttr, collAttr, errorAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc, limit,
	)

	var rows []SlowQueryPattern
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSlowestCollections(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]SlowCollectionRow, error) {
	fc, fargs := shared.FilterClauses(f)
	collAttr := shared.AttrString(shared.AttrDBCollectionName)
	errorAttr := shared.AttrString(shared.AttrErrorType)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS collection_name,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms,
		    toFloat64(sum(hist_count)) / %f                                                 AS ops_per_sec,
		    toFloat64(sumIf(hist_count, notEmpty(%s))) / nullIf(toFloat64(sum(hist_count)), 0) * 100 AS error_rate
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY collection_name
		ORDER BY p99_ms DESC
		LIMIT 50
	`,
		collAttr,
		bucketSec,
		errorAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		collAttr,
		fc,
	)

	var rows []SlowCollectionRow
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSlowQueryRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, thresholdMs float64) ([]SlowRatePoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	thresholdSec := thresholdMs / 1000.0

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                                    AS time_bucket,
		    toFloat64(sumIf(hist_count, (hist_sum / nullIf(hist_count, 0)) > %f)) / %f           AS slow_per_sec
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
		thresholdSec, bucketSec,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)

	var rows []SlowRatePoint
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetP99ByQueryText(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]P99ByQueryText, error) {
	if limit <= 0 {
		limit = 10
	}
	fc, fargs := shared.FilterClauses(f)
	queryAttr := shared.AttrString(shared.AttrDBQueryText)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS query_text,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY query_text
		ORDER BY p99_ms DESC
		LIMIT %d
	`,
		queryAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		queryAttr,
		fc, limit,
	)

	var rows []P99ByQueryText
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}
	return rows, nil
}
