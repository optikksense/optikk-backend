// Package explorer backs the POST /logs/query + /logs/analytics + GET
// /logs/:id read paths. CH reads split between raw `observability.logs`
// (list, get-by-id, body-search + attribute-filter paths) and the rollup
// cascade (volume trend, facets, rollup-friendly aggregations). Dispatch
// happens in querycompiler + the Target passed to Compile.
package explorer

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
)

const (
	rawLogsTable		= "observability.logs"
	// logsRollupPrefix/logsFacetRollupPrefix are rollup family constants — pass them
	// to rollup.For / rollup.For for tier resolution.
	logsRollupPrefix	= "logs_volume"
	logsFacetRollupPrefix	= "logs_facets"
	logColumns		= `timestamp, observed_timestamp, severity_text, severity_number, severity_bucket,
			body, trace_id, span_id, trace_flags,
			service, host, pod, container, environment,
			attributes_string, attributes_number, attributes_bool,
			scope_name, scope_version`
)

// Repository is the shared DB handle for all split repo files.
type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository	{ return &Repository{db: db} }

// ListLogs runs a keyset-paginated scan of raw logs.
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
	// PREWHERE on (team_id, ts_bucket_start) prunes partitions before the
	// rest of the predicates run — these two columns lead the MergeTree
	// sort key so the engine skips whole granules. The same predicates
	// still appear inside `where`; CH's optimiser dedupes at plan time.
	query := fmt.Sprintf(
		`SELECT %s FROM %s
		PREWHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		WHERE %s
		ORDER BY timestamp DESC, observed_timestamp DESC, trace_id DESC LIMIT @pgLimit`,
		logColumns, rawLogsTable, where,
	)
	args = append(args, clickhouse.Named("pgLimit", uint64(limit+1)))
	var rows []logRowDTO
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "logs.ListLogs", &rows, query, args...); err != nil {
		return nil, false, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	if os.Getenv("OPTIKK_DEBUG_LOGS_LIST") == "1" {
		slog.InfoContext(ctx, "logs ListLogs",
			"team_id", f.TeamID,
			"start_ms", f.StartMs,
			"end_ms", f.EndMs,
			"returned_rows", len(rows),
			"where", compiled.Where,
		)
	}
	return rows, hasMore, nil
}

// GetByID reads a single log by its deep-link key triple.
func (r *Repository) GetByID(ctx context.Context, teamID int64, traceID, spanID string, tsNs int64) (*logRowDTO, error) {
	// Compare timestamps in nanoseconds on the CH side. Binding DateTime64(9) from
	// Go time.Time can diverge slightly from toUnixTimestamp64Nano(timestamp) in
	// edge cases; list ids are built from scanned UnixNano, so this predicate
	// matches the encoded id reliably.
	query := fmt.Sprintf(
		`SELECT %s FROM %s
		PREWHERE team_id = @teamID AND trace_id = @traceID AND span_id = @spanID
		WHERE toUnixTimestamp64Nano(timestamp) = @tsNs LIMIT 1`,
		logColumns, rawLogsTable,
	)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
		clickhouse.Named("spanID", spanID),
		clickhouse.Named("tsNs", tsNs),
	}
	var rows []logRowDTO
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "explorer.GetByID", &rows, query, args...); err != nil {
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
