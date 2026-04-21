// Package collection serves DB-collection-scoped panels. Aggregate methods
// (GetCollectionLatency / GetCollectionOps / GetCollectionErrors /
// GetCollectionReadVsWrite) read `db_histograms_rollup`. GetCollectionQueryTexts
// groups by `attributes.db.query.text` — a free-text field whose cardinality
// equals the number of distinct queries observed (often millions). Rolling
// that up would simply mirror raw. Permanent raw.
package collection

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
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

// collectionFilter returns the fragment + arg pair that pins db_collection to
// the user-supplied name on top of the standard rollup filters.
func collectionFilter(collectionName string, f shared.Filters) (string, []any) {
	frag, args := shared.RollupFilterClauses(f)
	frag += ` AND db_collection = @collection`
	args = append(args, clickhouse.Named("collection", collectionName))
	return frag, args
}

func (r *ClickHouseRepository) GetCollectionLatency(ctx context.Context, teamID int64, startMs, endMs int64, collectionName string, f shared.Filters) ([]LatencyTimeSeries, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)
	fc, fargs := collectionFilter(collectionName, f)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    db_operation                                                                AS group_by,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1]) * 1000  AS p50_ms,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) * 1000  AS p95_ms,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) * 1000  AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)
	var rows []LatencyTimeSeries
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetCollectionOps(ctx context.Context, teamID int64, startMs, endMs int64, collectionName string, f shared.Filters) ([]OpsTimeSeries, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)
	fc, fargs := collectionFilter(collectionName, f)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                  AS time_bucket,
		    db_operation                        AS group_by,
		    toFloat64(sumMerge(hist_count)) / %f AS ops_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, bucketSec, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)
	var rows []OpsTimeSeries
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetCollectionErrors(ctx context.Context, teamID int64, startMs, endMs int64, collectionName string, f shared.Filters) ([]ErrorTimeSeries, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)
	fc, fargs := collectionFilter(collectionName, f)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                   AS time_bucket,
		    error_type                           AS group_by,
		    toFloat64(sumMerge(hist_count)) / %f AS errors_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND notEmpty(error_type)
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, bucketSec, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)
	var rows []ErrorTimeSeries
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetCollectionQueryTexts groups by `db.query.text` attribute which is not a
// rollup dim (too high-cardinality to include). Stays on raw metrics.
// TODO(phase8): if a bounded-text rollup lands, migrate this.
func (r *ClickHouseRepository) GetCollectionQueryTexts(ctx context.Context, teamID int64, startMs, endMs int64, collectionName string, f shared.Filters, limit int) ([]CollectionTopQuery, error) {
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

	params := append(shared.BaseParams(teamID, startMs, endMs), clickhouse.Named("collection", collectionName))
	params = append(params, fargs...)
	var rows []CollectionTopQuery
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, params...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetCollectionReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, collectionName string) ([]ReadWritePoint, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                                                          AS time_bucket,
		    toFloat64(sumMergeIf(hist_count, upper(db_operation) IN ('SELECT', 'FIND', 'GET'))) / %f                    AS read_ops_per_sec,
		    toFloat64(sumMergeIf(hist_count, upper(db_operation) IN ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'UPSERT', 'SET', 'PUT', 'AGGREGATE'))) / %f AS write_ops_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND db_collection = @collection
		GROUP BY time_bucket
		ORDER BY time_bucket
	`, shared.BucketTimeExpr, bucketSec, bucketSec, table)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
		clickhouse.Named("collection", collectionName),
	)
	var rows []ReadWritePoint
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// silence unused-import warning when this package keeps a raw fallback
var _ = timebucket.Expression
