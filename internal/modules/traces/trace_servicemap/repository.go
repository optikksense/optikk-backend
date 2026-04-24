package trace_servicemap	//nolint:revive,stylecheck

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/traceidmatch"
)

type Repository interface {
	GetServiceMapSpans(ctx context.Context, teamID int64, traceID string) ([]serviceMapSpanRow, error)
	GetTraceErrors(ctx context.Context, teamID int64, traceID string) ([]traceErrorRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetServiceMapSpans returns the minimal per-span projection needed to build a
// per-trace service map (client→server graph) in the service layer.
func (r *ClickHouseRepository) GetServiceMapSpans(ctx context.Context, teamID int64, traceID string) ([]serviceMapSpanRow, error) {
	var rows []serviceMapSpanRow
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_servicemap.GetServiceMapSpans", &rows, `
		SELECT span_id, parent_span_id, service_name,
		       duration_nano / 1000000.0 AS duration_ms, has_error
		FROM observability.spans
		WHERE team_id = @teamID AND `+traceidmatch.WhereTraceIDMatchesCH("trace_id", "traceID")+`
		ORDER BY timestamp ASC
		LIMIT 10000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

// GetTraceErrors returns the raw error-span rows for a trace. Aggregation into
// groups by exception_type happens in the service layer.
func (r *ClickHouseRepository) GetTraceErrors(ctx context.Context, teamID int64, traceID string) ([]traceErrorRow, error) {
	var rows []traceErrorRow
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_servicemap.GetTraceErrors", &rows, `
		SELECT span_id, service_name, name AS operation_name,
		       exception_type, exception_message, status_message,
		       timestamp AS start_time, duration_nano / 1000000.0 AS duration_ms
		FROM observability.spans
		WHERE team_id = @teamID AND `+traceidmatch.WhereTraceIDMatchesCH("trace_id", "traceID")+`
		  AND (has_error = true OR status_code_string = 'ERROR')
		ORDER BY timestamp ASC
		LIMIT 1000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}
