package tracedetail

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	rootspan "github.com/observability/observability-backend-go/internal/modules/spans/shared/rootspan"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

func (r *ClickHouseRepository) GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error) {
	var rows []spanAttributeRow
	if err := r.db.Select(ctx, &rows, `
		SELECT s.span_id, s.trace_id, s.name AS operation_name, s.service_name,
		       s.attributes_string, s.resource_attributes,
		       s.exception_type, s.exception_message, s.exception_stacktrace,
		       s.db_system, s.db_name, s.db_statement
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID AND s.span_id = @spanID
		LIMIT 1
	`, clickhouse.Named("teamID", uint32(teamID)), clickhouse.Named("traceID", traceID), clickhouse.Named("spanID", spanID)); err != nil { //nolint:gosec // G115
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	row := rows[0]
	return &row, nil
}

func (r *ClickHouseRepository) GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	var rows []RelatedTrace
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id, s.trace_id, s.name AS operation_name, s.service_name,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.status_code_string AS status, s.timestamp AS start_time
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+rootspan.Condition("s")+`
		  AND s.service_name = @serviceName
		  AND s.name = @operationName
		  AND s.trace_id != @excludeTraceID
		ORDER BY s.timestamp DESC
		LIMIT @limit
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("operationName", operationName),
		clickhouse.Named("excludeTraceID", excludeTraceID),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}
