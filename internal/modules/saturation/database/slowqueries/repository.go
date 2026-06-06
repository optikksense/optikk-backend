// Package slowqueries serves the slow-query panels by querying spans.
package slowqueries

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

type Repository interface {
	GetSlowQueryPatterns(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, limit int) ([]patternRawDTO, error)
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
	P50Ms          float32 `ch:"p50_ms"`
	P95Ms          float32 `ch:"p95_ms"`
	P99Ms          float32 `ch:"p99_ms"`
	CallCount      uint64  `ch:"call_count"`
	ErrorCount     uint64  `ch:"error_count"`
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
		           count()                                                                            AS call_count,
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

	args := append(filter.SpanArgs(teamID, startMs, endMs), clickhouse.Named("qLimit", uint64(limit)))
	args = append(args, filterArgs...)
	var rows []patternRawDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "slowqueries.GetSlowQueryPatterns", &rows, query, args...)
}
