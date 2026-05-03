package trace_shape //nolint:revive,stylecheck

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// Repository runs trace-id-scoped point lookups for trace-shape views.
// Queries only — span-kind percentage math and flamegraph self-time
// computation live in service.go.
type Repository interface {
	GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]spanKindDurationRow, error)
	GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]flamegraphRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]spanKindDurationRow, error) {
	const query = `
		SELECT kind_string                       AS span_kind,
		       sum(duration_nano) / 1000000.0    AS total_duration_ms,
		       toInt64(count())                  AS span_count
		FROM observability.spans
		PREWHERE team_id = @teamID
		WHERE ` + traceIDMatchPredicate + `
		GROUP BY kind_string
		ORDER BY total_duration_ms DESC`
	var rows []spanKindDurationRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_shape.GetSpanKindBreakdown", &rows, query, traceIDArgs(teamID, traceID)...)
}

func (r *ClickHouseRepository) GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]flamegraphRow, error) {
	const query = `
		SELECT span_id,
		       parent_span_id,
		       name                              AS operation_name,
		       service,
		       kind_string                       AS span_kind,
		       duration_nano / 1000000.0         AS duration_ms,
		       toUnixTimestamp64Nano(timestamp)  AS start_ns,
		       has_error
		FROM observability.spans
		PREWHERE team_id = @teamID
		WHERE ` + traceIDMatchPredicate + `
		ORDER BY start_ns ASC
		LIMIT 10000`
	var rows []flamegraphRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_shape.GetFlamegraphData", &rows, query, traceIDArgs(teamID, traceID)...)
}

// traceIDMatchPredicate normalizes trace_id across the empty string + 32-char
// all-zero hex sentinel forms OTLP allows, plus hex casing. Inlined per the
// project rule that retired the shared traces/shared/traceidmatch helper.
const traceIDMatchPredicate = `(
		lowerUTF8(trace_id) = lowerUTF8(@traceID)
		OR (length(@traceID) = 0 AND trace_id = '00000000000000000000000000000000')
		OR (lowerUTF8(@traceID) = '00000000000000000000000000000000' AND length(trace_id) = 0)
	)`

func traceIDArgs(teamID int64, traceID string) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
	}
}
