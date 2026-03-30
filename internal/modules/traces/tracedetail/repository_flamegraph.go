package tracedetail

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func (r *ClickHouseRepository) GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]flamegraphRow, error) {
	var rows []flamegraphRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id, s.parent_span_id, s.name AS operation_name,
		       s.service_name, s.kind_string AS span_kind,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       toUnixTimestamp64Nano(s.timestamp) AS start_ns,
		       s.has_error
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID
		ORDER BY start_ns ASC
		LIMIT 5000
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID)) //nolint:gosec // G115
	return rows, err
}
