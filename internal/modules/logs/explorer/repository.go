// Package explorer backs the POST /logs/query + /logs/analytics + GET
// /logs/:id read paths. CH reads split between raw `observability.logs_v2`
// (list, get-by-id, body-search + attribute-filter paths) and the rollup
// cascade (volume trend, facets, rollup-friendly aggregations). Dispatch
// happens in querycompiler + the Target passed to Compile.
package explorer

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
)

const (
	rawLogsTable       = "observability.logs_v2"
	logsRollupPrefix   = "observability.logs_rollup_v2"
	logsFacetRollupTbl = "observability.logs_facets_rollup_5m"
	logColumns         = `timestamp, observed_timestamp, severity_text, severity_number, severity_bucket,
			body, trace_id, span_id, trace_flags,
			service, host, pod, container, environment,
			attributes_string, attributes_number, attributes_bool,
			scope_name, scope_version`
)

// Repository is the shared DB handle for all split repo files.
type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// ListLogs runs a keyset-paginated scan of raw logs_v2.
func (r *Repository) ListLogs(ctx context.Context, f querycompiler.Filters, limit int, cur Cursor) ([]logRowDTO, bool, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetRaw)
	where := compiled.Where
	args := compiled.Args
	if !cur.IsZero() {
		where += ` AND (timestamp, observed_timestamp, trace_id) < (@curTs, @curOts, @curTid)`
		args = append(args,
			clickhouse.Named("curTs", cur.Timestamp),
			clickhouse.Named("curOts", cur.ObservedTimestamp),
			clickhouse.Named("curTid", cur.TraceID),
		)
	}
	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE %s ORDER BY timestamp DESC, observed_timestamp DESC, trace_id DESC LIMIT @pgLimit`,
		logColumns, rawLogsTable, where,
	)
	args = append(args, clickhouse.Named("pgLimit", uint64(limit+1)))
	var rows []logRowDTO
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, false, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, nil
}

// GetByID reads a single log by its deep-link key triple.
func (r *Repository) GetByID(ctx context.Context, teamID int64, traceID, spanID string, tsNs int64) (*logRowDTO, error) {
	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE team_id = @teamID AND trace_id = @traceID AND span_id = @spanID AND timestamp = @ts LIMIT 1`,
		logColumns, rawLogsTable,
	)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
		clickhouse.Named("spanID", spanID),
		clickhouse.Named("ts", time.Unix(0, tsNs)),
	}
	var rows []logRowDTO
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

// parseLogID decodes the base64url (trace_id:span_id:ts_ns) identifier.
func parseLogID(id string) (traceID, spanID string, tsNs int64, ok bool) {
	raw, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return "", "", 0, false
	}
	parts := strings.SplitN(string(raw), ":", 3)
	if len(parts) != 3 {
		return "", "", 0, false
	}
	ts, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return "", "", 0, false
	}
	return parts[0], parts[1], ts, true
}
