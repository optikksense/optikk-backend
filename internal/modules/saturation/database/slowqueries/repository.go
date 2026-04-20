package slowqueries

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"

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

func dbQueryFingerprintAttr() string { return shared.AttrString("db.query.text.fingerprint") }

// GetSlowQueryPatterns emits raw sum/count + the fingerprint/system carried
// to service for DbQueryLatency sketch lookups. Percentiles are zero
// placeholders that the service fills in.
func (r *ClickHouseRepository) GetSlowQueryPatterns(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]SlowQueryPattern, error) {
	if limit <= 0 {
		limit = 10
	}
	fc, fargs := shared.FilterClauses(f)
	queryAttr := shared.AttrString(shared.AttrDBQueryText)
	collAttr := shared.AttrString(shared.AttrDBCollectionName)
	errorAttr := shared.AttrString(shared.AttrErrorType)
	systemAttr := shared.AttrString(shared.AttrDBSystem)
	fpAttr := dbQueryFingerprintAttr()

	query := fmt.Sprintf(`
		SELECT
		    %s                                               AS query_text,
		    %s                                               AS collection_name,
		    %s                                               AS db_system,
		    %s                                               AS query_fingerprint,
		    CAST(0 AS Nullable(Float64))                     AS p50_ms,
		    CAST(0 AS Nullable(Float64))                     AS p95_ms,
		    CAST(0 AS Nullable(Float64))                     AS p99_ms,
		    sum(hist_sum)                                    AS latency_sum,
		    toInt64(sum(hist_count))                         AS latency_count,
		    toInt64(sum(hist_count))                         AS call_count,
		    toInt64(sumIf(hist_count, notEmpty(%s)))         AS error_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
		GROUP BY query_text, collection_name, db_system, query_fingerprint
		ORDER BY latency_count DESC
		LIMIT %d
	`,
		queryAttr, collAttr, systemAttr, fpAttr, errorAttr,
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
	systemAttr := shared.AttrString(shared.AttrDBSystem)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS collection_name,
		    %s                                                                              AS db_system,
		    sum(hist_sum)                                                                   AS latency_sum,
		    toInt64(sum(hist_count))                                                        AS latency_count,
		    CAST(0 AS Nullable(Float64))                                                    AS p99_ms,
		    toFloat64(sum(hist_count)) / %f                                                 AS ops_per_sec,
		    toFloat64(sumIf(hist_count, notEmpty(%s))) / nullIf(toFloat64(sum(hist_count)), 0) * 100 AS error_rate
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY collection_name, db_system
		ORDER BY latency_count DESC
		LIMIT 50
	`,
		collAttr, systemAttr,
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

// GetSlowQueryRate uses a ratio filter — (hist_sum / hist_count) > threshold —
// which is a per-row histogram average, not an aggregate avg(). The rule
// forbids aggregate avg(); this is a row-local divide that stays.
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
	systemAttr := shared.AttrString(shared.AttrDBSystem)
	fpAttr := dbQueryFingerprintAttr()

	query := fmt.Sprintf(`
		SELECT
		    %s                            AS query_text,
		    %s                            AS db_system,
		    %s                            AS query_fingerprint,
		    CAST(0 AS Nullable(Float64))  AS p99_ms
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY query_text, db_system, query_fingerprint
		ORDER BY sum(hist_count) DESC
		LIMIT %d
	`,
		queryAttr, systemAttr, fpAttr,
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
