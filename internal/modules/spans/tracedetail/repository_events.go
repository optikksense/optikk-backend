package tracedetail

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func (r *ClickHouseRepository) GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventRow, []exceptionRow, error) {
	var events []spanEventRow
	if err := r.db.Select(ctx, &events, `
		SELECT s.span_id, s.trace_id, s.timestamp, event_json
		FROM observability.spans s
		ARRAY JOIN s.events AS event_json
		WHERE s.team_id = @teamID AND s.trace_id = ?
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, clickhouse.Named("teamID", uint32(teamID)), traceID); err != nil {
		return nil, nil, err
	}

	var exceptions []exceptionRow
	if err := r.db.Select(ctx, &exceptions, `
		SELECT s.span_id, s.trace_id, s.timestamp,
		       s.exception_type, s.exception_message, s.exception_stacktrace
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = ?
		  AND (s.exception_type != '' OR s.exception_message != '' OR s.exception_stacktrace != '')
		ORDER BY s.timestamp ASC
		LIMIT 1000
	`, clickhouse.Named("teamID", uint32(teamID)), traceID); err != nil {
		return nil, nil, err
	}

	return events, exceptions, nil
}
