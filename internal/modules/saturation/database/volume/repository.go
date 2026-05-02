package volume

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

// Repository runs the volume / read-vs-write panel queries against raw
// `observability.spans`. One span per DB call → count() is the op count;
// service.go converts to per-second rate via filter.BucketWidthSeconds.
type Repository interface {
	GetOpsBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]opsRawDTO, error)
	GetOpsByOperation(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]opsRawDTO, error)
	GetOpsByCollection(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]opsRawDTO, error)
	GetOpsByNamespace(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]opsRawDTO, error)
	GetReadVsWrite(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]readWriteRawDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetOpsBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]opsRawDTO, error) {
	return r.opsSeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBSystem, "volume.GetOpsBySystem")
}

func (r *ClickHouseRepository) GetOpsByOperation(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]opsRawDTO, error) {
	return r.opsSeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBOperationName, "volume.GetOpsByOperation")
}

func (r *ClickHouseRepository) GetOpsByCollection(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]opsRawDTO, error) {
	return r.opsSeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBCollectionName, "volume.GetOpsByCollection")
}

func (r *ClickHouseRepository) GetOpsByNamespace(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]opsRawDTO, error) {
	return r.opsSeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBNamespace, "volume.GetOpsByNamespace")
}

func (r *ClickHouseRepository) opsSeriesByGroup(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, attr, traceLabel string) ([]opsRawDTO, error) {
	groupCol := filter.Spans1mGroupColumn(attr)
	if groupCol == "" {
		return nil, nil
	}
	filterWhere, filterArgs := filter.BuildSpans1mClauses(f)

	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(toDateTime(ts_bucket))    AS time_bucket,
		       ` + groupCol + `                   AS group_by,
		       toInt64(sum(request_count))        AS op_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''` + filterWhere + `
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	var rows []opsRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, traceLabel, &rows, query, args...)
}

// GetReadVsWrite splits ops by upper(db.operation.name) into read-style
// (SELECT/FIND/GET) vs write-style (INSERT/UPDATE/DELETE/REPLACE/UPSERT/SET/PUT/AGGREGATE).
// One span per call; counts split via countIf.
func (r *ClickHouseRepository) GetReadVsWrite(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]readWriteRawDTO, error) {
	filterWhere, filterArgs := filter.BuildSpans1mClauses(f)

	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(toDateTime(ts_bucket))                                                                                                              AS time_bucket,
		       toInt64(sumIf(request_count, upper(db_operation_name) IN ('SELECT','FIND','GET')))                                                            AS read_count,
		       toInt64(sumIf(request_count, upper(db_operation_name) IN ('INSERT','UPDATE','DELETE','REPLACE','UPSERT','SET','PUT','AGGREGATE')))            AS write_count
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''` + filterWhere + `
		GROUP BY time_bucket
		ORDER BY time_bucket`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	var rows []readWriteRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "volume.GetReadVsWrite", &rows, query, args...)
}
