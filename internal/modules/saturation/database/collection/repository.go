package collection

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

type Repository interface {
	GetCollectionLatency(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]latencyRawDTO, error)
	GetCollectionOps(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]opsRawDTO, error)
	GetCollectionErrors(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]opsRawDTO, error)
	GetCollectionQueryTexts(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters, limit int) ([]queryTextRawDTO, error)
	GetCollectionReadVsWrite(ctx context.Context, teamID, startMs, endMs int64, collection string) ([]readWriteRawDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type latencyRawDTO struct {
	TimeBucket string  `ch:"time_bucket"`
	GroupBy    string  `ch:"group_by"`
	P50Ms      float64 `ch:"p50_ms"`
	P95Ms      float64 `ch:"p95_ms"`
	P99Ms      float64 `ch:"p99_ms"`
}

type opsRawDTO struct {
	TimeBucket string `ch:"time_bucket"`
	GroupBy    string `ch:"group_by"`
	Count      uint64 `ch:"op_count"`
}

type queryTextRawDTO struct {
	QueryText  string   `ch:"query_text"`
	Buckets    []uint64 `ch:"bucket_counts"`
	CallCount  uint64   `ch:"call_count"`
	ErrorCount uint64   `ch:"error_count"`
}

type readWriteRawDTO struct {
	TimeBucket string `ch:"time_bucket"`
	ReadCount  uint64 `ch:"read_count"`
	WriteCount uint64 `ch:"write_count"`
}

func (r *ClickHouseRepository) GetCollectionLatency(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]latencyRawDTO, error) {
	filterWhere, filterArgs := filter.BuildSpans1mClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT time_bucket,
		       group_by,
		       qs[1] AS p50_ms,
		       qs[2] AS p95_ms,
		       qs[3] AS p99_ms
		FROM (
		    SELECT toString(toDateTime(ts_bucket))                       AS time_bucket,
		           db_operation_name                                     AS group_by,
		           quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state)  AS qs
		    FROM observability.spans_1m
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		    WHERE timestamp BETWEEN @start AND @end
		      AND db_collection_name = @collection` + filterWhere + `
		    GROUP BY time_bucket, group_by
		)
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	args = append(args, filterArgs...)
	var rows []latencyRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "collection.GetCollectionLatency", &rows, query, args...)
}

func (r *ClickHouseRepository) GetCollectionOps(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]opsRawDTO, error) {
	filterWhere, filterArgs := filter.BuildSpans1mClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(toDateTime(ts_bucket))   AS time_bucket,
		       db_operation_name                  AS group_by,
		       toUInt64(sum(request_count))       AS op_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_collection_name = @collection` + filterWhere + `
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	args = append(args, filterArgs...)
	var rows []opsRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "collection.GetCollectionOps", &rows, query, args...)
}

func (r *ClickHouseRepository) GetCollectionErrors(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]opsRawDTO, error) {
	filterWhere, filterArgs := filter.BuildSpans1mClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(toDateTime(ts_bucket))   AS time_bucket,
		       error_type                         AS group_by,
		       sum(error_count)                   AS op_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_collection_name = @collection` + filterWhere + `
		GROUP BY time_bucket, group_by
		HAVING op_count > 0
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	args = append(args, filterArgs...)
	var rows []opsRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "collection.GetCollectionErrors", &rows, query, args...)
}

func (r *ClickHouseRepository) GetCollectionQueryTexts(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters, limit int) ([]queryTextRawDTO, error) {
	if limit <= 0 {
		limit = 20
	}
	filterWhere, filterArgs := filter.BuildSpanClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT db_statement                                                                       AS query_text,
		       ` + filter.LatencyBucketCountsSQL() + `                                            AS bucket_counts,
		       toUInt64(count())                                                                  AS call_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                  AS error_count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND attributes.'db.collection.name'::String = @collection
		  AND db_statement != ''` + filterWhere + `
		GROUP BY query_text
		ORDER BY call_count DESC
		LIMIT @qLimit`

	args := append(filter.SpanArgs(teamID, startMs, endMs),
		clickhouse.Named("collection", collection),
		clickhouse.Named("qLimit", uint64(limit)), //nolint:gosec
	)
	args = append(args, filterArgs...)
	var rows []queryTextRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "collection.GetCollectionQueryTexts", &rows, query, args...)
}

func (r *ClickHouseRepository) GetCollectionReadVsWrite(ctx context.Context, teamID, startMs, endMs int64, collection string) ([]readWriteRawDTO, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(toDateTime(ts_bucket))                                                                                              AS time_bucket,
		       toUInt64(sumIf(request_count, upper(db_operation_name) IN ('SELECT','FIND','GET')))                                            AS read_count,
		       toUInt64(sumIf(request_count, upper(db_operation_name) IN ('INSERT','UPDATE','DELETE','REPLACE','UPSERT','SET','PUT','AGGREGATE'))) AS write_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_collection_name = @collection
		GROUP BY time_bucket
		ORDER BY time_bucket`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("collection", collection))
	var rows []readWriteRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "collection.GetCollectionReadVsWrite", &rows, query, args...)
}
