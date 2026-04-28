// Package slowqueries serves the slow-query panels. Queries operate over
// raw `observability.spans`: each DB call is one span with `duration_nano`,
// `db_statement`, and the OTel db.* attributes. No rollup table exists or
// is needed — the per-query-text grouping is the high-cardinality leaf.
package slowqueries

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

type Repository interface {
	GetSlowQueryPatterns(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, limit int) ([]patternRawDTO, error)
	GetSlowestCollections(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]slowCollRawDTO, error)
	GetSlowQueryRate(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, thresholdMs float64) ([]slowRateRawDTO, error)
	GetP99ByQueryText(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, limit int) ([]p99ByQueryRawDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type patternRawDTO struct {
	QueryText      string   `ch:"query_text"`
	CollectionName string   `ch:"collection_name"`
	Buckets        []uint64 `ch:"bucket_counts"`
	CallCount      uint64   `ch:"call_count"`
	ErrorCount     uint64   `ch:"error_count"`
}

type slowCollRawDTO struct {
	CollectionName string   `ch:"collection_name"`
	Buckets        []uint64 `ch:"bucket_counts"`
	CallCount      uint64   `ch:"call_count"`
	ErrorCount     uint64   `ch:"error_count"`
}

type slowRateRawDTO struct {
	TimeBucket string `ch:"time_bucket"`
	SlowCount  uint64 `ch:"slow_count"`
}

type p99ByQueryRawDTO struct {
	QueryText string   `ch:"query_text"`
	Buckets   []uint64 `ch:"bucket_counts"`
}

func (r *ClickHouseRepository) GetSlowQueryPatterns(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, limit int) ([]patternRawDTO, error) {
	if limit <= 0 {
		limit = 10
	}
	filterWhere, filterArgs := filter.BuildSpanClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT db_statement                                                                       AS query_text,
		       attributes.'db.collection.name'::String                                            AS collection_name,
		       ` + filter.LatencyBucketCountsSQL() + `                                            AS bucket_counts,
		       toUInt64(count())                                                                  AS call_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                  AS error_count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''
		  AND db_statement != ''` + filterWhere + `
		GROUP BY query_text, collection_name
		ORDER BY call_count DESC
		LIMIT @qLimit`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("qLimit", uint64(limit))) //nolint:gosec
	args = append(args, filterArgs...)
	var rows []patternRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slowqueries.GetSlowQueryPatterns", &rows, query, args...)
}

// GetSlowestCollections returns per-collection histogram + counts; service
// computes p99 + ops/sec + error_rate and orders by p99 desc top-50.
func (r *ClickHouseRepository) GetSlowestCollections(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]slowCollRawDTO, error) {
	filterWhere, filterArgs := filter.BuildSpanClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT attributes.'db.collection.name'::String                                            AS collection_name,
		       ` + filter.LatencyBucketCountsSQL() + `                                            AS bucket_counts,
		       toUInt64(count())                                                                  AS call_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                  AS error_count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''
		  AND attributes.'db.collection.name'::String != ''` + filterWhere + `
		GROUP BY collection_name
		LIMIT 200`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	var rows []slowCollRawDTO
	return rows, dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "slowqueries.GetSlowestCollections", &rows, query, args...)
}

// GetSlowQueryRate counts spans whose duration exceeds the threshold per
// 5-minute time bucket. Service converts to per-second rate.
func (r *ClickHouseRepository) GetSlowQueryRate(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, thresholdMs float64) ([]slowRateRawDTO, error) {
	filterWhere, filterArgs := filter.BuildSpanClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(toDateTime(ts_bucket))                                       AS time_bucket,
		       countIf(duration_nano / 1000000.0 > @thresholdMs)                     AS slow_count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''` + filterWhere + `
		GROUP BY time_bucket
		ORDER BY time_bucket`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("thresholdMs", thresholdMs))
	args = append(args, filterArgs...)
	var rows []slowRateRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slowqueries.GetSlowQueryRate", &rows, query, args...)
}

// GetP99ByQueryText groups by db_statement; service computes p99 from the
// histogram and orders desc, top-N.
func (r *ClickHouseRepository) GetP99ByQueryText(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, limit int) ([]p99ByQueryRawDTO, error) {
	if limit <= 0 {
		limit = 10
	}
	filterWhere, filterArgs := filter.BuildSpanClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT db_statement                            AS query_text,
		       ` + filter.LatencyBucketCountsSQL() + ` AS bucket_counts
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''
		  AND db_statement != ''` + filterWhere + `
		GROUP BY query_text
		LIMIT 500`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	var rows []p99ByQueryRawDTO
	_ = limit // service trims after sorting
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slowqueries.GetP99ByQueryText", &rows, query, args...)
}
