package explorer

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

const traceIndexColumns = `trace_id,
		timestamp                                                  AS start_ms,
		timestamp                                                  AS end_ms,
		duration_nano                                              AS duration_ns,
		service                                                    AS root_service,
		name                                                       AS root_operation,
		status_code_string                                         AS root_status,
		http_method                                                AS root_http_method,
		response_status_code                                       AS root_http_status,
		1                                                          AS span_count,
		has_error,
		(CASE WHEN has_error THEN 1 ELSE 0 END)                    AS error_count,
		[service]                                                  AS service_set,
		false                                                      AS truncated,
		timestamp                                                  AS last_seen_ms`

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// ListTraces reads root spans for the per-trace summary list.
func (r *Repository) ListTraces(ctx context.Context, f filter.Filters, limit int, cur TraceCursor) ([]traceIndexRowDTO, bool, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	if cur.TraceID != "" {
		where += ` AND (timestamp, trace_id) < (@curStart, @curTraceID)`
		args = append(args,
			clickhouse.Named("curStart", time.UnixMilli(int64(cur.StartMs))),
			clickhouse.Named("curTraceID", cur.TraceID),
		)
	}
	args = append(args, clickhouse.Named("pgLimit", uint64(limit+1)))

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT ` + traceIndexColumns + `
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end AND is_root = 1` + where + `
		ORDER BY timestamp DESC, trace_id DESC
		LIMIT @pgLimit`

	var rows []traceIndexRowDTO
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "traces.ListTraces", &rows, query, args...); err != nil {
		return nil, false, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, nil
}

// GetByID reads a single trace summary by trace_id (root span). PREWHERE on
// team_id so partition elimination happens before the bloom on trace_id kicks
// in.
func (r *Repository) GetByID(ctx context.Context, teamID int64, traceID string) (*traceIndexRowDTO, error) {
	const query = `
		SELECT ` + traceIndexColumns + `
		FROM observability.spans
		PREWHERE team_id = @teamID
		WHERE trace_id = @traceID AND is_root = 1
		ORDER BY timestamp DESC
		LIMIT 1`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
	}
	var rows []traceIndexRowDTO
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "explorer.GetByID", &rows, query, args...); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

// Summarize returns total / error / duration totals over the filtered window.
func (r *Repository) Summarize(ctx context.Context, f filter.Filters) (Summary, error) {
	resourceWhere, where, args := filter.BuildClauses(f)

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT count()              AS t,
		       countIf(has_error)   AS e,
		       sum(duration_nano)   AS d
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end AND is_root = 1` + where

	var row struct {
		T uint64 `ch:"t"`
		E uint64 `ch:"e"`
		D uint64 `ch:"d"`
	}
	if err := dbutil.QueryRowCH(dbutil.ExplorerCtx(ctx), r.db, "explorer.Summarize", &row, query, args...); err != nil {
		return Summary{}, err
	}
	return Summary{TotalTraces: row.T, TotalErrors: row.E, TotalDuration: row.D}, nil
}

