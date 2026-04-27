// Package slowqueries serves the slow-query panels. GetSlowQueryRate reads
// `db_histograms_rollup`. GetSlowQueryPatterns + GetP99ByQueryText group by
// `attributes.db.query.text` — high-cardinality free-text; rolling up would
// just mirror raw. Permanent raw (same rationale as collection.GetCollectionQueryTexts).
package slowqueries

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
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
		    quantileTDigestWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p50_ms,
		    quantileTDigestWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p95_ms,
		    quantileTDigestWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms,
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
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slowqueries.GetSlowQueryPatterns", &rows, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSlowestCollections(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]SlowCollectionRow, error) {
	table := "observability.metrics"
	fc, fargs := shared.RollupFilterClauses(f)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    db_collection                                                               AS collection_name,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) * 1000  AS p99_ms,
		    toFloat64(sum(hist_count)) / %f                                        AS ops_per_sec,
		    toFloat64(sumMergeIf(hist_count, notEmpty(error_type))) / nullIf(toFloat64(sum(hist_count)), 0) * 100 AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND notEmpty(db_collection)
		  %s
		GROUP BY collection_name
		ORDER BY p99_ms DESC
		LIMIT 50
	`, bucketSec, table, fc)

	args := shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration)
	args = append(args, fargs...)
	var rows []SlowCollectionRow
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "slowqueries.GetSlowestCollections", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetSlowQueryRate approximates slow-ops/sec via `db_histograms_rollup`'s
// t-digest merge: whole buckets whose p95 exceeds the threshold contribute
// their full `hist_count` as slow; faster buckets are filtered out via
// HAVING. Coarser than the raw per-row `sumIf(hist_count, latency>threshold)`
// (which the rollup can't express because per-row latencies collapse into the
// digest), but bucket-level accurate and rollup-compatible.
func (r *ClickHouseRepository) GetSlowQueryRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, thresholdMs float64) ([]SlowRatePoint, error) {
	table := "observability.signoz_index_v3"
	fc, fargs := shared.RollupFilterClauses(f)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                  AS time_bucket,
		    toFloat64(sum(hist_count)) / %f                                AS slow_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket
		HAVING toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) > @thresholdMs
		ORDER BY time_bucket
	`,
		shared.BucketTimeExpr, bucketSec, table, fc,
	)
	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(60, startMs, endMs)),
		clickhouse.Named("thresholdMs", thresholdMs),
	)
	args = append(args, fargs...)
	var rows []SlowRatePoint
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slowqueries.GetSlowQueryRate", &rows, query, args...); err != nil {
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
		    quantileTDigestWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms
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
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slowqueries.GetP99ByQueryText", &rows, query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return nil, err
	}
	return rows, nil
}
