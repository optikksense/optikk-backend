package system

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

// Repository runs the per-DB-system panels. Every method PREWHEREs raw
// `observability.spans` on `(team_id, ts_bucket, fingerprint IN active_fps)`
// and pins `db_system = @dbSystem`. Latency methods emit fixed-bucket
// histogram arrays — service.go interpolates P50/P95/P99 Go-side.
type Repository interface {
	GetSystemLatency(ctx context.Context, teamID, startMs, endMs int64, dbSystem string, f filter.Filters) ([]latencyRawDTO, error)
	GetSystemOps(ctx context.Context, teamID, startMs, endMs int64, dbSystem string, f filter.Filters) ([]opsRawDTO, error)
	GetSystemErrors(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]opsRawDTO, error)
	GetSystemTopCollectionsByLatency(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]collectionLatencyRawDTO, error)
	GetSystemTopCollectionsByVolume(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]collectionLatencyRawDTO, error)
	GetSystemNamespaces(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error)
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

type collectionLatencyRawDTO struct {
	CollectionName string  `ch:"collection_name"`
	P99Ms          float64 `ch:"p99_ms"`
	Count          uint64  `ch:"op_count"`
}

func (r *ClickHouseRepository) GetSystemLatency(ctx context.Context, teamID, startMs, endMs int64, dbSystem string, f filter.Filters) ([]latencyRawDTO, error) {
	filterWhere, filterArgs := filter.BuildSpans1mClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(toDateTime(ts_bucket))                       AS time_bucket,
		       db_operation_name                                     AS group_by,
		       quantileTimingMerge(0.5)(latency_state)               AS p50_ms,
		       quantileTimingMerge(0.95)(latency_state)              AS p95_ms,
		       quantileTimingMerge(0.99)(latency_state)              AS p99_ms
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system = @dbSystem` + filterWhere + `
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	args = append(args, filterArgs...)
	var rows []latencyRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemLatency", &rows, query, args...)
}

func (r *ClickHouseRepository) GetSystemOps(ctx context.Context, teamID, startMs, endMs int64, dbSystem string, f filter.Filters) ([]opsRawDTO, error) {
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
		  AND db_system = @dbSystem` + filterWhere + `
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	args = append(args, filterArgs...)
	var rows []opsRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemOps", &rows, query, args...)
}

func (r *ClickHouseRepository) GetSystemErrors(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]opsRawDTO, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(toDateTime(ts_bucket))   AS time_bucket,
		       db_operation_name                  AS group_by,
		       sum(error_count)                   AS op_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system = @dbSystem
		GROUP BY time_bucket, group_by
		HAVING op_count > 0
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	var rows []opsRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemErrors", &rows, query, args...)
}

// GetSystemTopCollectionsByLatency returns per-collection histogram + total
// ops count; service.go interpolates p99 from bucket counts and orders by
// p99 desc (top 20).
func (r *ClickHouseRepository) GetSystemTopCollectionsByLatency(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]collectionLatencyRawDTO, error) {
	return r.collectionLatencyTop(ctx, teamID, startMs, endMs, dbSystem, "system.GetSystemTopCollectionsByLatency")
}

// GetSystemTopCollectionsByVolume — same shape; service.go orders by
// op_count desc (top 20).
func (r *ClickHouseRepository) GetSystemTopCollectionsByVolume(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]collectionLatencyRawDTO, error) {
	return r.collectionLatencyTop(ctx, teamID, startMs, endMs, dbSystem, "system.GetSystemTopCollectionsByVolume")
}

func (r *ClickHouseRepository) collectionLatencyTop(ctx context.Context, teamID, startMs, endMs int64, dbSystem, traceLabel string) ([]collectionLatencyRawDTO, error) {
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT db_collection_name                              AS collection_name,
		       quantileTimingMerge(0.99)(latency_state)        AS p99_ms,
		       sum(request_count)                              AS op_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system = @dbSystem
		  AND db_collection_name != ''
		GROUP BY collection_name
		LIMIT 200`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	var rows []collectionLatencyRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, traceLabel, &rows, query, args...)
}

func (r *ClickHouseRepository) GetSystemNamespaces(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT db_namespace                       AS namespace,
		       toInt64(sum(request_count))        AS span_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system = @dbSystem
		  AND db_namespace != ''
		GROUP BY namespace
		ORDER BY span_count DESC`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	var rows []SystemNamespace
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemNamespaces", &rows, query, args...)
}
