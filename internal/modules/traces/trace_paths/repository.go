package trace_paths	//nolint:revive,stylecheck

import (
	"context"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/traceidmatch"
)

type Repository interface {
	GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]criticalPathRow, error)
	GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]errorPathRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]criticalPathRow, error) {
	// Phase 7: read from spans_by_trace_index MV — narrow range scan keyed on
	// (team_id, trace_id, span_id) instead of a bloom-filter guess on raw spans.
	var rows []criticalPathRow
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_paths.GetCriticalPath", &rows, `
		SELECT s.span_id, s.parent_span_id,
		       s.name AS operation_name,
		       s.service,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       toUnixTimestamp64Nano(s.timestamp) AS start_ns,
		       toUnixTimestamp64Nano(s.timestamp) + s.duration_nano AS end_ns
		FROM observability.spans s
		PREWHERE s.team_id = @teamID
		WHERE `+traceidmatch.WhereTraceIDMatchesCH("s.trace_id", "traceID")+`
		ORDER BY start_ns ASC
		LIMIT 5000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

func (r *ClickHouseRepository) GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]errorPathRow, error) {
	var rows []errorPathRow
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_paths.GetErrorPath", &rows, `
		SELECT s.span_id, s.parent_span_id, s.name AS operation_name,
		       s.service AS service, s.status_code_string AS status, s.status_message,
		       s.timestamp AS start_time, s.duration_nano / 1000000.0 AS duration_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID AND `+traceidmatch.WhereTraceIDMatchesCH("s.trace_id", "traceID")+`
		  AND (s.has_error = true OR s.status_code_string = 'ERROR')
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

// isRootParentSpanID treats empty string and all-zero hex as "no parent" — both
// forms appear in real data depending on SDK and ingest path.
func isRootParentSpanID(parentID string) bool {
	trimmed := strings.Trim(parentID, "\x00")
	return trimmed == "" || trimmed == "0000000000000000"
}
