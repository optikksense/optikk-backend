// Package slowqueries serves the slow-query panels. Queries operate over
// raw `observability.spans`: each DB call is one span with `duration_nano`,
// `db_statement`, and the OTel db.* attributes. No rollup table exists or
// is needed — the per-query-text grouping is the high-cardinality leaf.
package slowqueries

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
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
	QueryText      string  `ch:"query_text"`
	CollectionName string  `ch:"collection_name"`
	P50Ms          float64 `ch:"p50_ms"`
	P95Ms          float64 `ch:"p95_ms"`
	P99Ms          float64 `ch:"p99_ms"`
	CallCount      uint64  `ch:"call_count"`
	ErrorCount     uint64  `ch:"error_count"`
}

type slowCollRawDTO struct {
	CollectionName string  `ch:"collection_name"`
	P99Ms          float64 `ch:"p99_ms"`
	OpsPerSec      float64 `ch:"ops_per_sec"`
	ErrorRatePct   float64 `ch:"error_rate_pct"`
	HasCalls       uint8   `ch:"has_calls"`
}

type slowRateRawDTO struct {
	TimeBucket string  `ch:"time_bucket"`
	SlowPerSec float64 `ch:"slow_per_sec"`
}

type p99ByQueryRawDTO struct {
	QueryText string  `ch:"query_text"`
	P99Ms     float64 `ch:"p99_ms"`
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
		),
		grouped AS (
		    SELECT db_statement                                                                       AS query_text,
		           attributes.'db.collection.name'::String                                            AS collection_name,
		           quantileTimingState(duration_nano / 1000000.0)                                     AS lat_state,
		           toUInt64(count())                                                                  AS call_count,
		           countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                  AS error_count
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		    WHERE timestamp BETWEEN @start AND @end
		      AND db_system != ''
		      AND db_statement != ''` + filterWhere + `
		    GROUP BY query_text, collection_name
		)
		SELECT query_text,
		       collection_name,
		       qs[1]      AS p50_ms,
		       qs[2]      AS p95_ms,
		       qs[3]      AS p99_ms,
		       call_count,
		       error_count
		FROM (
		    SELECT query_text,
		           collection_name,
		           any(call_count)                                  AS call_count,
		           any(error_count)                                 AS error_count,
		           quantilesTimingMerge(0.5, 0.95, 0.99)(lat_state) AS qs
		    FROM grouped
		    GROUP BY query_text, collection_name
		)
		ORDER BY call_count DESC
		LIMIT @qLimit`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("qLimit", uint64(limit))) //nolint:gosec
	args = append(args, filterArgs...)
	var rows []patternRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slowqueries.GetSlowQueryPatterns", &rows, query, args...)
}

// GetSlowestCollections returns the top-50 per-collection rows
// (p99_ms DESC, collection_name ASC) ranked + emitted server-side.
// ops_per_sec divides per-bucket counts by the display-grain seconds.
// error_rate_pct is computed inline; has_calls flags zero-traffic rows
// so the service can emit nil instead of a real 0.0.
func (r *ClickHouseRepository) GetSlowestCollections(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]slowCollRawDTO, error) {
	filterWhere, filterArgs := filter.BuildSpans1mClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT db_collection_name                                AS collection_name,
		       quantileTimingMerge(0.99)(latency_state)          AS p99_ms,
		       sum(request_count) / @bucketGrainSec              AS ops_per_sec,
		       if(sum(request_count) > 0,
		          sum(error_count) * 100.0 / sum(request_count),
		          0.0)                                           AS error_rate_pct,
		       if(sum(request_count) > 0, toUInt8(1), toUInt8(0)) AS has_calls
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''
		  AND db_collection_name != ''` + filterWhere + `
		GROUP BY collection_name
		ORDER BY p99_ms DESC, collection_name ASC
		LIMIT 50`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	args = timebucket.WithBucketGrainSec(args, startMs, endMs)
	var rows []slowCollRawDTO
	return rows, dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "slowqueries.GetSlowestCollections", &rows, query, args...)
}

// GetSlowQueryRate counts spans whose duration exceeds the threshold,
// folded to the display-grain bucket and emitted as a per-second rate.
func (r *ClickHouseRepository) GetSlowQueryRate(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, thresholdMs float64) ([]slowRateRawDTO, error) {
	filterWhere, filterArgs := filter.BuildSpanClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(` + timebucket.DisplayGrainSQL(endMs-startMs) + `)            AS time_bucket,
		       countIf(duration_nano / 1000000.0 > @thresholdMs) / @bucketGrainSec    AS slow_per_sec
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''` + filterWhere + `
		GROUP BY time_bucket
		ORDER BY time_bucket`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("thresholdMs", thresholdMs))
	args = append(args, filterArgs...)
	args = timebucket.WithBucketGrainSec(args, startMs, endMs)
	var rows []slowRateRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slowqueries.GetSlowQueryRate", &rows, query, args...)
}

// GetP99ByQueryText groups by db_statement; quantile + ranking are
// computed server-side via the quantileTimingState / quantileTimingMerge
// pair followed by ORDER BY p99_ms DESC LIMIT @qLimit. Service is
// pass-through.
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
		),
		grouped AS (
		    SELECT db_statement                                  AS query_text,
		           quantileTimingState(duration_nano / 1000000.0) AS lat_state
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		    WHERE timestamp BETWEEN @start AND @end
		      AND db_system != ''
		      AND db_statement != ''` + filterWhere + `
		    GROUP BY query_text
		)
		SELECT query_text,
		       quantileTimingMerge(0.99)(lat_state) AS p99_ms
		FROM grouped
		GROUP BY query_text
		ORDER BY p99_ms DESC, query_text ASC
		LIMIT @qLimit`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	args = append(args, clickhouse.Named("qLimit", uint64(limit))) //nolint:gosec
	var rows []p99ByQueryRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slowqueries.GetP99ByQueryText", &rows, query, args...)
}
