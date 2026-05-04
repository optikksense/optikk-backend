package system

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

// Repository runs the per-DB-system panels. Every method PREWHEREs
// `observability.spans_1m` on `(team_id, ts_bucket, fingerprint IN active_fps)`
// and pins `db_system = @dbSystem`. Latency methods emit server-side
// percentiles via `quantilesTimingMerge`; ops/errors emit per-second rates
// via `sum(...) / @bucketGrainSec` at the display grain.
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
	TimeBucket string  `ch:"time_bucket"`
	GroupBy    string  `ch:"group_by"`
	OpsPerSec  float64 `ch:"ops_per_sec"`
}

type collectionLatencyRawDTO struct {
	CollectionName string  `ch:"collection_name"`
	P99Ms          float64 `ch:"p99_ms"`
	OpsPerSec      float64 `ch:"ops_per_sec"`
}

func (r *ClickHouseRepository) GetSystemLatency(ctx context.Context, teamID, startMs, endMs int64, dbSystem string, f filter.Filters) ([]latencyRawDTO, error) {
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
		      AND db_system = @dbSystem` + filterWhere + `
		    GROUP BY time_bucket, group_by
		)
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
		SELECT toString(` + timebucket.DisplayGrainSQL(endMs-startMs) + `)         AS time_bucket,
		       db_operation_name                                                   AS group_by,
		       sum(request_count) / @bucketGrainSec                                AS ops_per_sec
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system = @dbSystem` + filterWhere + `
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	args = append(args, filterArgs...)
	args = timebucket.WithBucketGrainSec(args, startMs, endMs)
	var rows []opsRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemOps", &rows, query, args...)
}

func (r *ClickHouseRepository) GetSystemErrors(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]opsRawDTO, error) {
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(` + timebucket.DisplayGrainSQL(endMs-startMs) + `)         AS time_bucket,
		       db_operation_name                                                   AS group_by,
		       sum(error_count) / @bucketGrainSec                                  AS ops_per_sec
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system = @dbSystem
		GROUP BY time_bucket, group_by
		HAVING ops_per_sec > 0
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	args = timebucket.WithBucketGrainSec(args, startMs, endMs)
	var rows []opsRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemErrors", &rows, query, args...)
}

// GetSystemTopCollectionsByLatency returns the top-20 per-collection
// (p99_ms DESC, collection_name ASC) ranked + emitted server-side.
func (r *ClickHouseRepository) GetSystemTopCollectionsByLatency(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]collectionLatencyRawDTO, error) {
	return r.collectionLatencyTop(ctx, teamID, startMs, endMs, dbSystem,
		"p99_ms DESC, collection_name ASC", "system.GetSystemTopCollectionsByLatency")
}

// GetSystemTopCollectionsByVolume returns the top-20 per-collection
// (ops_per_sec DESC, collection_name ASC) ranked + emitted server-side.
func (r *ClickHouseRepository) GetSystemTopCollectionsByVolume(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]collectionLatencyRawDTO, error) {
	return r.collectionLatencyTop(ctx, teamID, startMs, endMs, dbSystem,
		"ops_per_sec DESC, collection_name ASC", "system.GetSystemTopCollectionsByVolume")
}

func (r *ClickHouseRepository) collectionLatencyTop(ctx context.Context, teamID, startMs, endMs int64, dbSystem, orderBy, traceLabel string) ([]collectionLatencyRawDTO, error) {
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT db_collection_name                              AS collection_name,
		       quantileTimingMerge(0.99)(latency_state)        AS p99_ms,
		       sum(request_count) / @bucketGrainSec            AS ops_per_sec
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system = @dbSystem
		  AND db_collection_name != ''
		GROUP BY collection_name
		ORDER BY ` + orderBy + `
		LIMIT 20`

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("dbSystem", dbSystem))
	args = timebucket.WithBucketGrainSec(args, startMs, endMs)
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
