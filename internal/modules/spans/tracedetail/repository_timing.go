package tracedetail

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func (r *ClickHouseRepository) GetSpanSelfTimes(ctx context.Context, teamID int64, traceID string) ([]SpanSelfTime, error) {
	var rows []SpanSelfTime
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id,
		       s.name AS operation_name,
		       s.duration_nano / 1000000.0 AS total_duration_ms,
		       coalesce(child_sum.child_ms, 0) AS child_time_ms,
		       greatest(0, s.duration_nano / 1000000.0 - coalesce(child_sum.child_ms, 0)) AS self_time_ms
		FROM observability.spans s
		LEFT JOIN (
		    SELECT parent_span_id, sum(duration_nano) / 1000000.0 AS child_ms
		    FROM observability.spans
		    WHERE team_id = @teamID AND trace_id = @traceID
		    GROUP BY parent_span_id
		) AS child_sum ON child_sum.parent_span_id = s.span_id
		WHERE s.team_id = @teamID AND s.trace_id = @traceID
		ORDER BY self_time_ms DESC
		LIMIT 5000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID))
	return rows, err
}

func (r *ClickHouseRepository) GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]errorPathRow, error) {
	var rows []errorPathRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id, s.parent_span_id, s.name AS operation_name,
		       s.service_name AS service_name, s.status_code_string AS status, s.status_message,
		       s.timestamp AS start_time, s.duration_nano / 1000000.0 AS duration_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID
		  AND (s.has_error = true OR s.status_code_string = 'ERROR')
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID))
	return rows, err
}
