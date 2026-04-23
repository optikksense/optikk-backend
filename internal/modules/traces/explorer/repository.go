// Package explorer backs the POST /traces/query + /traces/analytics + GET
// /traces/:id read paths. List + facets + trend read observability.traces_index
// (per-trace summary). Analytics and raw-span filters fall through to
// observability.spans + spans_rollup_*.
package explorer

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

const (
	tracesIndexTable   = "observability.traces_index"
	spansRawTable      = "observability.spans"
	spansRollupPrefix  = "observability.spans_rollup"
	tracesFacetRollup  = "observability.traces_facets_rollup_5m"
	traceIndexColumns  = `trace_id, start_ms, end_ms, duration_ns, root_service, root_operation, root_status,
			root_http_method, root_http_status, span_count, has_error, error_count, service_set, truncated, last_seen_ms`
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// ListTraces reads observability.traces_index (per-trace summaries from the spans indexer).
func (r *Repository) ListTraces(ctx context.Context, f querycompiler.Filters, limit int, cur TraceCursor) ([]traceIndexRowDTO, bool, []string, error) {
	return r.listTracesIndex(ctx, f, limit, cur)
}

func (r *Repository) listTracesIndex(ctx context.Context, f querycompiler.Filters, limit int, cur TraceCursor) ([]traceIndexRowDTO, bool, []string, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetTracesIndex)
	where := compiled.Where
	args := compiled.Args
	if cur.TraceID != "" {
		where += ` AND (start_ms, trace_id) < (@curStart, @curTraceID)`
		args = append(args,
			clickhouse.Named("curStart", cur.StartMs),
			clickhouse.Named("curTraceID", cur.TraceID),
		)
	}
	query := fmt.Sprintf(
		`SELECT %s FROM %s PREWHERE %s WHERE %s ORDER BY start_ms DESC, trace_id DESC LIMIT @pgLimit`,
		traceIndexColumns, tracesIndexTable, compiled.PreWhere, where,
	)
	args = append(args, clickhouse.Named("pgLimit", uint64(limit+1))) //nolint:gosec
	var rows []traceIndexRowDTO
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, false, compiled.DroppedClauses, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, compiled.DroppedClauses, nil
}

// GetByID reads a single trace summary from traces_index.
func (r *Repository) GetByID(ctx context.Context, teamID int64, traceID string) (*traceIndexRowDTO, error) {
	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE team_id = @teamID AND trace_id = @traceID ORDER BY last_seen_ms DESC LIMIT 1`,
		traceIndexColumns, tracesIndexTable,
	)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("traceID", traceID),
	}
	var rows []traceIndexRowDTO
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

// Summarize returns totals from traces_index.
func (r *Repository) Summarize(ctx context.Context, f querycompiler.Filters) (Summary, error) {
	return r.summarizeTracesIndex(ctx, f)
}

func (r *Repository) summarizeTracesIndex(ctx context.Context, f querycompiler.Filters) (Summary, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetTracesIndex)
	query := fmt.Sprintf(
		`SELECT count() AS t, countIf(has_error) AS e, sum(duration_ns) AS d FROM %s PREWHERE %s WHERE %s`,
		tracesIndexTable, compiled.PreWhere, compiled.Where,
	)
	var row struct {
		T uint64 `ch:"t"`
		E uint64 `ch:"e"`
		D uint64 `ch:"d"`
	}
	rows, err := r.db.Query(dbutil.ExplorerCtx(ctx), query, compiled.Args...)
	if err != nil {
		return Summary{}, err
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&row.T, &row.E, &row.D); err != nil {
			return Summary{}, err
		}
	}
	return Summary{TotalTraces: row.T, TotalErrors: row.E, TotalDuration: row.D}, nil
}
