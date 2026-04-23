package trace_shape //nolint:revive,stylecheck

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/traceidmatch"
)

type Repository interface {
	GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]spanKindDurationRow, error)
	GetSpanSelfTimes(ctx context.Context, teamID int64, traceID string) ([]SpanSelfTime, error)
	GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]flamegraphRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]spanKindDurationRow, error) {
	var rows []spanKindDurationRow
	err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, `
		SELECT kind_string                        AS span_kind,
		       sum(duration_nano) / 1000000.0     AS total_duration_ms,
		       toInt64(count())                   AS span_count
		FROM observability.spans
		WHERE team_id = @teamID AND `+traceidmatch.WhereTraceIDMatchesCH("trace_id", "traceID")+`
		GROUP BY kind_string
		ORDER BY total_duration_ms DESC
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

func (r *ClickHouseRepository) GetSpanSelfTimes(ctx context.Context, teamID int64, traceID string) ([]SpanSelfTime, error) {
	var rows []SpanSelfTime
	err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, `
		SELECT s.span_id, s.name AS operation_name,
		       s.duration_nano / 1000000.0 AS total_duration_ms,
		       (s.duration_nano - coalesce(cs.child_duration, 0)) / 1000000.0 AS self_time_ms,
		       coalesce(cs.child_duration, 0) / 1000000.0 AS child_time_ms
		FROM observability.spans s
		LEFT JOIN (
			SELECT parent_span_id, sum(duration_nano) AS child_duration
			FROM observability.spans
			WHERE team_id = @teamID AND `+traceidmatch.WhereTraceIDMatchesCH("trace_id", "traceID")+`
			GROUP BY parent_span_id
		) cs ON s.span_id = cs.parent_span_id
		WHERE s.team_id = @teamID AND `+traceidmatch.WhereTraceIDMatchesCH("s.trace_id", "traceID")+`
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

func (r *ClickHouseRepository) GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]flamegraphRow, error) {
	var rows []flamegraphRow
	err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, `
		SELECT span_id, parent_span_id, name AS operation_name, service_name,
		       kind_string AS span_kind, duration_nano / 1000000.0 AS duration_ms,
		       toUnixTimestamp64Nano(timestamp) AS start_ns, has_error
		FROM observability.spans
		WHERE team_id = @teamID AND `+traceidmatch.WhereTraceIDMatchesCH("trace_id", "traceID")+`
		ORDER BY start_ns ASC
		LIMIT 10000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}
