package span_query //nolint:revive,stylecheck

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

const spanRowColumns = `span_id, trace_id, parent_span_id, service, name, kind_string,
		duration_nano, toUnixTimestamp64Nano(timestamp) AS timestamp_ns, has_error,
		status_code_string, http_method, response_status_code, environment`

type Repository interface {
	ListSpans(ctx context.Context, f filter.Filters, limit int, cur SpanCursor) ([]spanRowDTO, bool, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// ListSpans reads individual spans from observability.spans with keyset
// pagination on (timestamp, span_id). Resource-dim filters flow through the
// inline `WITH active_fps AS (... spans_resource ...)` CTE.
func (r *ClickHouseRepository) ListSpans(ctx context.Context, f filter.Filters, limit int, cur SpanCursor) ([]spanRowDTO, bool, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	if cur.SpanID != "" {
		where += ` AND (toUnixTimestamp64Nano(timestamp), span_id) < (@curTs, @curSpanID)`
		args = append(args,
			clickhouse.Named("curTs", cur.TimestampNs),
			clickhouse.Named("curSpanID", cur.SpanID),
		)
	}
	args = append(args, clickhouse.Named("pgLimit", uint64(limit+1))) //nolint:gosec

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT ` + spanRowColumns + `
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end` + where + `
		ORDER BY timestamp DESC, span_id DESC
		LIMIT @pgLimit`

	var rows []spanRowDTO
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "span_query.ListSpans", &rows, query, args...); err != nil {
		return nil, false, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, nil
}
