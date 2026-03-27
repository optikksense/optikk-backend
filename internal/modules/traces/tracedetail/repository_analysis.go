package tracedetail

import (
	"context"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func isRootParentSpanID(parentID string) bool {
	trimmed := strings.Trim(parentID, "\x00")
	return trimmed == "" || trimmed == "0000000000000000"
}

func (r *ClickHouseRepository) GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]spanKindDurationRow, error) {
	var rows []spanKindDurationRow
	err := r.db.Select(ctx, &rows, `
		SELECT kind_string                        AS span_kind,
		       sum(duration_nano) / 1000000.0     AS total_duration_ms,
		       toInt64(count())                   AS span_count
		FROM observability.spans
		WHERE team_id = @teamID AND trace_id = @traceID
		GROUP BY kind_string
		ORDER BY total_duration_ms DESC
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}

func (r *ClickHouseRepository) GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]criticalPathRow, error) {
	var rows []criticalPathRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id, s.parent_span_id,
		       s.name AS operation_name,
		       s.service_name,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       toUnixTimestamp64Nano(s.timestamp) AS start_ns,
		       toUnixTimestamp64Nano(s.timestamp) + s.duration_nano AS end_ns
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID
		ORDER BY start_ns ASC
		LIMIT 5000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}
