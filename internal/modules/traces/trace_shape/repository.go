package trace_shape	//nolint:revive,stylecheck

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/traceidmatch"
)

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
	// Phase 7: narrow range scan on spans_by_trace_index.
	var rows []spanKindDurationRow
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_shape.GetSpanKindBreakdown", &rows, `
		SELECT kind_string                        AS span_kind,
		       sum(duration_nano) / 1000000.0     AS total_duration_ms,
		       toInt64(count())                   AS span_count
		FROM observability.signoz_index_v3
		PREWHERE team_id = @teamID
		WHERE `+traceidmatch.WhereTraceIDMatchesCH("trace_id", "traceID")+`
		GROUP BY kind_string
		ORDER BY total_duration_ms DESC
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

func (r *ClickHouseRepository) GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]flamegraphRow, error) {
	// Phase 7: narrow range scan on spans_by_trace_index.
	var rows []flamegraphRow
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_shape.GetFlamegraphData", &rows, `
		SELECT span_id, parent_span_id, name AS operation_name, service_name,
		       kind_string AS span_kind, duration_nano / 1000000.0 AS duration_ms,
		       toUnixTimestamp64Nano(timestamp) AS start_ns, has_error
		FROM observability.signoz_index_v3
		PREWHERE team_id = @teamID
		WHERE `+traceidmatch.WhereTraceIDMatchesCH("trace_id", "traceID")+`
		ORDER BY start_ns ASC
		LIMIT 10000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}
