package volume

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

// Repository runs the volume / read-vs-write panel queries against
// `observability.spans_1m`. SQL emits per-display-bucket rows with
// per-second rates computed server-side as
// `sum(request_count) / @bucketGrainSec`; service is pass-through.
type Repository interface {
	GetOpsBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]opsRawDTO, error)
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
		SELECT ` + timebucket.DisplayGrainSQL(endMs-startMs) + `                   AS time_bucket,
		       ` + groupCol + `                                                    AS group_by,
		       sum(request_count) / @bucketGrainSec                                AS ops_per_sec
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''` + filterWhere + `
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	args = timebucket.WithBucketGrainSec(args, startMs, endMs)
	var rows []opsRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, traceLabel, &rows, query, args...)
}
