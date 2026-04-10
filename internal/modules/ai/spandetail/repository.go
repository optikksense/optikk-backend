package spandetail

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const genAIFilter = `(attributes.'gen_ai.system'::String != '' OR attributes.'gen_ai.request.model'::String != '')`

// Repository defines data-access for the AI span detail module.
type Repository interface {
	GetSpanDetail(ctx context.Context, teamID int64, spanID string) (spanDetailRow, error)
	GetMessages(ctx context.Context, teamID int64, spanID string) ([]messageRow, error)
	GetTraceContext(ctx context.Context, teamID int64, traceID string) ([]traceSpanRow, error)
	GetRelatedSpans(ctx context.Context, teamID int64, model, operation string, excludeSpanID string, startMs, endMs int64, limit int) ([]relatedSpanRow, error)
	GetTokenBreakdown(ctx context.Context, teamID int64, spanID, model string, startMs, endMs int64) (tokenBreakdownRow, error)
}

// ClickHouseRepository implements Repository.
type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

// NewRepository creates a new ClickHouseRepository.
func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSpanDetail(ctx context.Context, teamID int64, spanID string) (spanDetailRow, error) {
	var rows []spanDetailRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id                                                   AS span_id,
		       s.trace_id                                                  AS trace_id,
		       s.parent_span_id                                            AS parent_span_id,
		       s.service_name                                              AS service_name,
		       s.name                                                      AS operation_name,
		       s.kind_string                                               AS kind_string,
		       s.timestamp                                                 AS timestamp,
		       duration_nano / 1000000.0                                   AS duration_ms,
		       s.has_error                                                 AS has_error,
		       s.status_message                                            AS status_message,
		       attributes.'gen_ai.request.model'::String                   AS model,
		       attributes.'gen_ai.response.model'::String                  AS response_model,
		       attributes.'gen_ai.system'::String                          AS provider,
		       attributes.'gen_ai.operation.name'::String                  AS operation_type,
		       attributes.'gen_ai.request.temperature'::Float64            AS temperature,
		       attributes.'gen_ai.request.top_p'::Float64                  AS top_p,
		       attributes.'gen_ai.request.max_tokens'::Int64               AS max_tokens,
		       attributes.'gen_ai.request.frequency_penalty'::Float64      AS frequency_penalty,
		       attributes.'gen_ai.request.presence_penalty'::Float64       AS presence_penalty,
		       attributes.'gen_ai.request.seed'::Int64                     AS seed,
		       attributes.'gen_ai.usage.input_tokens'::Int64               AS input_tokens,
		       attributes.'gen_ai.usage.output_tokens'::Int64              AS output_tokens,
		       attributes.'gen_ai.response.finish_reasons'::String         AS finish_reason,
		       attributes.'gen_ai.response.id'::String                     AS response_id,
		       attributes.'server.address'::String                         AS server_address,
		       attributes.'gen_ai.conversation.id'::String                 AS conversation_id
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.span_id = @spanID
		LIMIT 1
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("spanID", spanID),
	)
	if err != nil || len(rows) == 0 {
		return spanDetailRow{}, err
	}
	return rows[0], nil
}

func (r *ClickHouseRepository) GetMessages(ctx context.Context, teamID int64, spanID string) ([]messageRow, error) {
	var rows []messageRow
	err := r.db.Select(ctx, &rows, `
		SELECT event_name, body
		FROM observability.span_events
		WHERE team_id = @teamID AND span_id = @spanID
		  AND (event_name = 'gen_ai.content.prompt' OR event_name = 'gen_ai.content.completion')
		ORDER BY timestamp ASC
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("spanID", spanID),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetTraceContext(ctx context.Context, teamID int64, traceID string) ([]traceSpanRow, error) {
	var rows []traceSpanRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id                                                AS span_id,
		       s.parent_span_id                                         AS parent_span_id,
		       s.service_name                                           AS service_name,
		       s.name                                                   AS operation_name,
		       s.kind_string                                            AS kind_string,
		       s.timestamp                                              AS timestamp,
		       duration_nano / 1000000.0                                AS duration_ms,
		       s.has_error                                              AS has_error,
		       if(`+genAIFilter+`, true, false)                         AS is_ai
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID
		ORDER BY s.timestamp ASC
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetRelatedSpans(ctx context.Context, teamID int64, model, operation string, excludeSpanID string, startMs, endMs int64, limit int) ([]relatedSpanRow, error) {
	var rows []relatedSpanRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id                                                AS span_id,
		       s.timestamp                                              AS timestamp,
		       duration_nano / 1000000.0                                AS duration_ms,
		       attributes.'gen_ai.usage.input_tokens'::Int64            AS input_tokens,
		       attributes.'gen_ai.usage.output_tokens'::Int64           AS output_tokens,
		       s.has_error                                              AS has_error,
		       attributes.'gen_ai.response.finish_reasons'::String      AS finish_reason
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND attributes.'gen_ai.request.model'::String = @model
		  AND attributes.'gen_ai.operation.name'::String = @operation
		  AND s.span_id != @excludeSpanID
		ORDER BY s.timestamp DESC
		LIMIT @limit
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("model", model),
		clickhouse.Named("operation", operation),
		clickhouse.Named("excludeSpanID", excludeSpanID),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetTokenBreakdown(ctx context.Context, teamID int64, spanID, model string, startMs, endMs int64) (tokenBreakdownRow, error) {
	var rows []tokenBreakdownRow
	err := r.db.Select(ctx, &rows, `
		SELECT s1.input_tokens AS input_tokens,
		       s1.output_tokens AS output_tokens,
		       s2.avg_input AS avg_input_model,
		       s2.avg_output AS avg_output_model
		FROM (
			SELECT attributes.'gen_ai.usage.input_tokens'::Int64  AS input_tokens,
			       attributes.'gen_ai.usage.output_tokens'::Int64 AS output_tokens
			FROM observability.spans
			WHERE team_id = @teamID AND span_id = @spanID
			LIMIT 1
		) s1
		CROSS JOIN (
			SELECT avg(attributes.'gen_ai.usage.input_tokens'::Int64)  AS avg_input,
			       avg(attributes.'gen_ai.usage.output_tokens'::Int64) AS avg_output
			FROM observability.spans
			WHERE team_id = @teamID
			  AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND timestamp BETWEEN @start AND @end
			  AND attributes.'gen_ai.request.model'::String = @model
		) s2
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("spanID", spanID),
		clickhouse.Named("model", model),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	if err != nil || len(rows) == 0 {
		return tokenBreakdownRow{}, err
	}
	return rows[0], nil
}
