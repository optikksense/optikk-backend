package shape

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
		WITH trace_loc AS (
		    SELECT ts_bucket, fingerprint
		    FROM observability.trace_index
		    PREWHERE trace_id = @traceID AND team_id = @teamID
		)
		SELECT kind_string                       AS span_kind,
		       sum(duration_nano) / 1000000.0    AS total_duration_ms,
		       count()                           AS span_count
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND (ts_bucket, fingerprint) IN (SELECT ts_bucket, fingerprint FROM trace_loc)
		     AND trace_id = @traceID
		GROUP BY kind_string
		ORDER BY total_duration_ms DESC`
	var rows []spanKindDurationRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "shape.GetSpanKindBreakdown", &rows, query, traceIDArgs(teamID, traceID)...)
}

func (r *ClickHouseRepository) GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]flamegraphRow, error) {
	const query = `
		WITH trace_loc AS (
		    SELECT ts_bucket, fingerprint
		    FROM observability.trace_index
		    PREWHERE trace_id = @traceID AND team_id = @teamID
		)
		SELECT span_id,
		       parent_span_id,
		       name                              AS operation_name,
		       service,
		       kind_string                       AS span_kind,
		       duration_nano / 1000000.0         AS duration_ms,
		       timestamp                         AS timestamp,
		       has_error
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND (ts_bucket, fingerprint) IN (SELECT ts_bucket, fingerprint FROM trace_loc)
		     AND trace_id = @traceID
		ORDER BY timestamp ASC
		LIMIT 10000`
	var rows []flamegraphRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "shape.GetFlamegraphData", &rows, query, traceIDArgs(teamID, traceID)...)
}

func traceIDArgs(teamID int64, traceID string) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
	}
}
