// Package explorer backs the POST /traces/query + /traces/analytics + GET
// /traces/:id read paths. List + facets + trend read observability.traces_index
// (per-trace summary). Analytics and raw-span filters fall through to
// observability.signoz_index_v3 + spans_rollup_*.
package explorer

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
		"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

const (
	spansRawTable		= "observability.signoz_index_v3"
	traceIndexColumns	= `trace_id, timestamp AS start_ms, timestamp AS end_ms, duration_nano AS duration_ns, service_name AS root_service, name AS root_operation, status_code_string AS root_status,
			http_method AS root_http_method, response_status_code AS root_http_status, 1 AS span_count, has_error, (CASE WHEN has_error THEN 1 ELSE 0 END) AS error_count, [service_name] AS service_set, false AS truncated, timestamp AS last_seen_ms`
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository	{ return &Repository{db: db} }

// ListTraces reads observability.traces_index (per-trace summaries from the spans indexer).
func (r *Repository) ListTraces(ctx context.Context, f querycompiler.Filters, limit int, cur TraceCursor) ([]traceIndexRowDTO, bool, []string, error) {
	return r.listTracesIndex(ctx, f, limit, cur)
}

func (r *Repository) listTracesIndex(ctx context.Context, f querycompiler.Filters, limit int, cur TraceCursor) ([]traceIndexRowDTO, bool, []string, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetSpansRaw)
	where := compiled.Where + " AND is_root = 1"
	args := compiled.Args
	if cur.TraceID != "" {
		where += ` AND (start_ms, trace_id) < (@curStart, @curTraceID)`
		args = append(args,
			clickhouse.Named("curStart", cur.StartMs),
			clickhouse.Named("curTraceID", cur.TraceID),
		)
	}
	query := fmt.Sprintf(
		`SELECT %s FROM %s PREWHERE %s WHERE %s ORDER BY timestamp DESC, trace_id DESC LIMIT @pgLimit`,
		traceIndexColumns, spansRawTable, compiled.PreWhere, where,
	)
	args = append(args, clickhouse.Named("pgLimit", uint64(limit+1))) //nolint:gosec
	var rows []traceIndexRowDTO
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "traces.ListTraces", &rows, query, args...); err != nil {
		return nil, false, compiled.DroppedClauses, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, compiled.DroppedClauses, nil
}

// GetByID reads a single trace summary from traces_index. PREWHERE on team_id
// so partition elimination happens before the bloom filter on trace_id kicks in.
func (r *Repository) GetByID(ctx context.Context, teamID int64, traceID string) (*traceIndexRowDTO, error) {
	query := fmt.Sprintf(
		`SELECT %s FROM %s PREWHERE team_id = @teamID WHERE trace_id = @traceID AND is_root = 1 ORDER BY timestamp DESC LIMIT 1`,
		traceIndexColumns, spansRawTable,
	)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
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

// Summarize returns totals from traces_index.
func (r *Repository) Summarize(ctx context.Context, f querycompiler.Filters) (Summary, error) {
	return r.summarizeTracesIndex(ctx, f)
}

func (r *Repository) summarizeTracesIndex(ctx context.Context, f querycompiler.Filters) (Summary, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetSpansRaw)
	query := fmt.Sprintf(
		`SELECT count() AS t, countIf(has_error) AS e, sum(duration_nano) AS d FROM %s PREWHERE %s WHERE %s AND is_root = 1`,
		spansRawTable, compiled.PreWhere, compiled.Where,
	)
	var row struct {
		T	uint64	`ch:"t"`
		E	uint64	`ch:"e"`
		D	uint64	`ch:"d"`
	}
	rows, err := dbutil.QueryCH(dbutil.ExplorerCtx(ctx), r.db, "explorer.summarizeTracesIndex", query, compiled.Args...)
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
