// Package explorer backs the POST /traces/query + GET /traces/:id read paths.
// Reads `observability.spans` directly with `is_root = 1` and column
// aliases that shape rows into a per-trace summary. Resource-scoped filters
// (Services, ExcludeServices, Environments) flow through
// internal/modules/traces/shared/resource.WithFingerprints into a
// `fingerprint IN (...)` PREWHERE for ~99% scan reduction on wide
// windows. The previously-planned `observability.traces_index` summary table is
// not implemented; aliases here serve the same shape.
package explorer

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/resource"
)

const (
	spansRawTable		= "observability.spans"
	traceIndexColumns	= `trace_id, timestamp AS start_ms, timestamp AS end_ms, duration_nano AS duration_ns, service AS root_service, name AS root_operation, status_code_string AS root_status,
			http_method AS root_http_method, response_status_code AS root_http_status, 1 AS span_count, has_error, (CASE WHEN has_error THEN 1 ELSE 0 END) AS error_count, [service] AS service_set, false AS truncated, timestamp AS last_seen_ms`
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
	preWhere, args, empty, err := resource.WithFingerprints(ctx, r.db, f, compiled.PreWhere, compiled.Args)
	if err != nil {
		return nil, false, compiled.DroppedClauses, err
	}
	if empty {
		return nil, false, compiled.DroppedClauses, nil
	}
	where := compiled.Where + " AND is_root = 1"
	if cur.TraceID != "" {
		where += ` AND (start_ms, trace_id) < (@curStart, @curTraceID)`
		args = append(args,
			clickhouse.Named("curStart", cur.StartMs),
			clickhouse.Named("curTraceID", cur.TraceID),
		)
	}
	query := fmt.Sprintf(
		`SELECT %s FROM %s PREWHERE %s WHERE %s ORDER BY timestamp DESC, trace_id DESC LIMIT @pgLimit`,
		traceIndexColumns, spansRawTable, preWhere, where,
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
	preWhere, args, empty, err := resource.WithFingerprints(ctx, r.db, f, compiled.PreWhere, compiled.Args)
	if err != nil {
		return Summary{}, err
	}
	if empty {
		return Summary{}, nil
	}
	query := fmt.Sprintf(
		`SELECT count() AS t, countIf(has_error) AS e, sum(duration_nano) AS d FROM %s PREWHERE %s WHERE %s AND is_root = 1`,
		spansRawTable, preWhere, compiled.Where,
	)
	var row struct {
		T	uint64	`ch:"t"`
		E	uint64	`ch:"e"`
		D	uint64	`ch:"d"`
	}
	rows, qErr := dbutil.QueryCH(dbutil.ExplorerCtx(ctx), r.db, "explorer.summarizeTracesIndex", query, args...)
	if qErr != nil {
		return Summary{}, qErr
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&row.T, &row.E, &row.D); err != nil {
			return Summary{}, err
		}
	}
	return Summary{TotalTraces: row.T, TotalErrors: row.E, TotalDuration: row.D}, nil
}
