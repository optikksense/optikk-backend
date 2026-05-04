package errors

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

// Repository runs the error panel queries against `observability.spans_1m`.
// `has_error OR toUInt16OrZero(response_status_code) >= 400` is the canonical
// error predicate (consistent with services/errors and topology). SQL emits
// per-display-bucket per-second rates server-side; service is pass-through.
type Repository interface {
	GetErrorsBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRawDTO, error)
	GetErrorsByOperation(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRawDTO, error)
	GetErrorsByErrorType(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRawDTO, error)
	GetErrorsByCollection(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRawDTO, error)
	GetErrorsByResponseStatus(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRawDTO, error)
	GetErrorRatio(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRatioRawDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type errorRawDTO struct {
	TimeBucket   string  `ch:"time_bucket"`
	GroupBy      string  `ch:"group_by"`
	ErrorsPerSec float64 `ch:"errors_per_sec"`
}

type errorRatioRawDTO struct {
	TimeBucket    string  `ch:"time_bucket"`
	ErrorRatioPct float64 `ch:"error_ratio_pct"`
	HasData       uint8   `ch:"has_data"`
}

func (r *ClickHouseRepository) GetErrorsBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRawDTO, error) {
	return r.errorSeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBSystem, "errors.GetErrorsBySystem")
}

func (r *ClickHouseRepository) GetErrorsByOperation(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRawDTO, error) {
	return r.errorSeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBOperationName, "errors.GetErrorsByOperation")
}

func (r *ClickHouseRepository) GetErrorsByErrorType(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRawDTO, error) {
	return r.errorSeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrErrorType, "errors.GetErrorsByErrorType")
}

func (r *ClickHouseRepository) GetErrorsByCollection(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRawDTO, error) {
	return r.errorSeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBCollectionName, "errors.GetErrorsByCollection")
}

func (r *ClickHouseRepository) GetErrorsByResponseStatus(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRawDTO, error) {
	return r.errorSeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBResponseStatus, "errors.GetErrorsByResponseStatus")
}

func (r *ClickHouseRepository) errorSeriesByGroup(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, attr, traceLabel string) ([]errorRawDTO, error) {
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
		SELECT toString(` + timebucket.DisplayGrainSQL(endMs-startMs) + `)         AS time_bucket,
		       ` + groupCol + `                                                    AS group_by,
		       sum(error_count) / @bucketGrainSec                                  AS errors_per_sec
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''` + filterWhere + `
		GROUP BY time_bucket, group_by
		HAVING errors_per_sec > 0
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	args = timebucket.WithBucketGrainSec(args, startMs, endMs)
	var rows []errorRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, traceLabel, &rows, query, args...)
}

// GetErrorRatio emits per-display-bucket (err / total) * 100 directly.
// `has_data` flags whether `total_count > 0` so the service can distinguish
// "no traffic" (NULL ratio) from "0% errors" (real 0).
func (r *ClickHouseRepository) GetErrorRatio(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]errorRatioRawDTO, error) {
	filterWhere, filterArgs := filter.BuildSpans1mClauses(f)

	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toString(` + timebucket.DisplayGrainSQL(endMs-startMs) + `)                                  AS time_bucket,
		       if(sum(request_count) > 0, sum(error_count) * 100.0 / sum(request_count), 0.0)               AS error_ratio_pct,
		       if(sum(request_count) > 0, toUInt8(1), toUInt8(0))                                           AS has_data
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''` + filterWhere + `
		GROUP BY time_bucket
		ORDER BY time_bucket`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	var rows []errorRatioRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "errors.GetErrorRatio", &rows, query, args...)
}
