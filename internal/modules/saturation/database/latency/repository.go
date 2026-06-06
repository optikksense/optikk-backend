package latency

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

// Repository runs latency-by-* panels against `observability.spans_1m`.
// It computes percentiles via quantileTimingMerge on latency_state.
type Repository interface {
	GetLatencyBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type latencyRawDTO struct {
	BucketAt time.Time `ch:"bucket_at"`
	GroupBy  string    `ch:"group_by"`
	QS       []float32 `ch:"qs"`
	P50Ms    float32
	P95Ms    float32
	P99Ms    float32
}

func (r *ClickHouseRepository) GetLatencyBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]latencyRawDTO, error) {
	return r.latencySeriesByGroup(ctx, teamID, startMs, endMs, f, filter.AttrDBSystem, "latency.GetLatencyBySystem")
}

func (r *ClickHouseRepository) latencySeriesByGroup(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, attr, traceLabel string) ([]latencyRawDTO, error) {
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
		SELECT ` + timebucket.DisplayGrainSQL(endMs-startMs) + ` AS bucket_at,
		       ` + groupCol + `                                       AS group_by,
		       quantilesTimingMerge(0.5, 0.95, 0.99)(latency_state)  AS qs
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''` + filterWhere + `
		GROUP BY bucket_at, group_by
		ORDER BY bucket_at, group_by`

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	var rows []latencyRawDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, traceLabel, &rows, query, args...); err != nil {
		return nil, err
	}
	for i := range rows {
		if len(rows[i].QS) >= 3 {
			rows[i].P50Ms = rows[i].QS[0]
			rows[i].P95Ms = rows[i].QS[1]
			rows[i].P99Ms = rows[i].QS[2]
		}
	}
	return rows, nil
}
