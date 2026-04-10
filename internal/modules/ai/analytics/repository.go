package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const genAIFilter = `(attributes.'gen_ai.system'::String != '' OR attributes.'gen_ai.request.model'::String != '')`

// Repository defines data-access for the AI analytics module.
type Repository interface {
	GetModelCatalog(ctx context.Context, f AnalyticsFilter) ([]modelCatalogRow, error)
	GetLatencyDistribution(ctx context.Context, f AnalyticsFilter) ([]latencyBucketRow, error)
	GetParameterImpact(ctx context.Context, f AnalyticsFilter) ([]paramImpactRow, error)
	GetModelTimeseries(ctx context.Context, f AnalyticsFilter, model string) ([]modelTimeseriesRow, error)
	GetCostByModel(ctx context.Context, f AnalyticsFilter) ([]costRow, error)
	GetCostTimeseries(ctx context.Context, f AnalyticsFilter) ([]costTimeseriesRow, error)
	GetTokenEconomics(ctx context.Context, f AnalyticsFilter) (tokenEconomicsRow, error)
	GetErrorPatterns(ctx context.Context, f AnalyticsFilter, limit int) ([]errorPatternRow, error)
	GetErrorTimeseries(ctx context.Context, f AnalyticsFilter) ([]errorTimeseriesRow, error)
	GetFinishReasonTrends(ctx context.Context, f AnalyticsFilter) ([]finishReasonTrendRow, error)
	GetConversations(ctx context.Context, f AnalyticsFilter, limit, offset int) ([]conversationListRow, error)
	GetConversationTurns(ctx context.Context, teamID int64, conversationID string) ([]conversationTurnRow, error)
	GetConversationSummary(ctx context.Context, teamID int64, conversationID string) (conversationSummaryRow, error)
}

// ClickHouseRepository implements Repository.
type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

// NewRepository creates a new ClickHouseRepository.
func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func baseArgs(f AnalyticsFilter) []any {
	return []any{
		clickhouse.Named("teamID", uint32(f.TeamID)),                                //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(f.StartMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(f.EndMs/1000)),
		clickhouse.Named("start", time.UnixMilli(f.StartMs)),
		clickhouse.Named("end", time.UnixMilli(f.EndMs)),
	}
}

func optionalFilters(f AnalyticsFilter) (string, []any) {
	clause := ""
	var args []any
	if f.Service != "" {
		clause += " AND s.service_name = @filterService"
		args = append(args, clickhouse.Named("filterService", f.Service))
	}
	if f.Model != "" {
		clause += " AND attributes.'gen_ai.request.model'::String = @filterModel"
		args = append(args, clickhouse.Named("filterModel", f.Model))
	}
	if f.Provider != "" {
		clause += " AND attributes.'gen_ai.system'::String = @filterProvider"
		args = append(args, clickhouse.Named("filterProvider", f.Provider))
	}
	if f.Operation != "" {
		clause += " AND attributes.'gen_ai.operation.name'::String = @filterOp"
		args = append(args, clickhouse.Named("filterOp", f.Operation))
	}
	return clause, args
}

func allArgs(f AnalyticsFilter) (string, []any) {
	extra, extraArgs := optionalFilters(f)
	args := baseArgs(f)
	args = append(args, extraArgs...)
	return extra, args
}

// ---- model catalog ----

func (r *ClickHouseRepository) GetModelCatalog(ctx context.Context, f AnalyticsFilter) ([]modelCatalogRow, error) {
	extra, args := allArgs(f)
	var rows []modelCatalogRow
	err := r.db.Select(ctx, &rows, `
		SELECT attributes.'gen_ai.request.model'::String            AS model,
		       any(attributes.'gen_ai.system'::String)              AS provider,
		       toInt64(count())                                     AS request_count,
		       avg(duration_nano / 1000000.0)                       AS avg_latency_ms,
		       quantileExact(0.50)(duration_nano / 1000000.0)       AS p50_ms,
		       quantileExact(0.95)(duration_nano / 1000000.0)       AS p95_ms,
		       quantileExact(0.99)(duration_nano / 1000000.0)       AS p99_ms,
		       countIf(has_error = true) * 100.0 / count()          AS error_rate,
		       toInt64(sum(attributes.'gen_ai.usage.input_tokens'::Int64))  AS input_tokens,
		       toInt64(sum(attributes.'gen_ai.usage.output_tokens'::Int64)) AS output_tokens,
		       arrayStringConcat(topK(3)(attributes.'gen_ai.operation.name'::String), ', ') AS top_ops,
		       arrayStringConcat(topK(3)(service_name), ', ')       AS top_svcs
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY model
		ORDER BY request_count DESC
	`, args...)
	return rows, err
}

// ---- latency distribution ----

func (r *ClickHouseRepository) GetLatencyDistribution(ctx context.Context, f AnalyticsFilter) ([]latencyBucketRow, error) {
	extra, args := allArgs(f)
	var rows []latencyBucketRow
	err := r.db.Select(ctx, &rows, `
		SELECT attributes.'gen_ai.request.model'::String AS model,
		       toInt64(floor(duration_nano / 1000000.0 / 100) * 100) AS bucket_ms,
		       toInt64(count()) AS count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY model, bucket_ms
		ORDER BY model, bucket_ms
	`, args...)
	return rows, err
}

// ---- parameter impact ----

func (r *ClickHouseRepository) GetParameterImpact(ctx context.Context, f AnalyticsFilter) ([]paramImpactRow, error) {
	extra, args := allArgs(f)
	var rows []paramImpactRow
	err := r.db.Select(ctx, &rows, `
		SELECT round(attributes.'gen_ai.request.temperature'::Float64, 1) AS temperature,
		       avg(duration_nano / 1000000.0)                              AS avg_latency,
		       countIf(has_error = true) * 100.0 / count()                 AS error_rate,
		       toInt64(count())                                            AS count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		  AND attributes.'gen_ai.request.temperature'::Float64 > 0
		GROUP BY temperature
		ORDER BY temperature
	`, args...)
	return rows, err
}

// ---- model timeseries ----

func (r *ClickHouseRepository) GetModelTimeseries(ctx context.Context, f AnalyticsFilter, model string) ([]modelTimeseriesRow, error) {
	extra, args := allArgs(f)
	args = append(args, clickhouse.Named("targetModel", model))
	bucket := timebucket.ExprForColumnTime(f.StartMs, f.EndMs, "s.timestamp")
	var rows []modelTimeseriesRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s                                                AS timestamp,
		       toInt64(count())                                  AS request_count,
		       avg(duration_nano / 1000000.0)                    AS avg_latency_ms,
		       quantileExact(0.95)(duration_nano / 1000000.0)    AS p95_ms,
		       countIf(has_error = true) * 100.0 / count()       AS error_rate,
		       toInt64(sum(attributes.'gen_ai.usage.input_tokens'::Int64))  AS input_tokens,
		       toInt64(sum(attributes.'gen_ai.usage.output_tokens'::Int64)) AS output_tokens
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		  AND attributes.'gen_ai.request.model'::String = @targetModel
		GROUP BY timestamp
		ORDER BY timestamp ASC
	`, bucket), args...)
	return rows, err
}

// ---- cost by model ----

func (r *ClickHouseRepository) GetCostByModel(ctx context.Context, f AnalyticsFilter) ([]costRow, error) {
	extra, args := allArgs(f)
	var rows []costRow
	err := r.db.Select(ctx, &rows, `
		SELECT attributes.'gen_ai.request.model'::String                    AS model,
		       toInt64(sum(attributes.'gen_ai.usage.input_tokens'::Int64))  AS input_tokens,
		       toInt64(sum(attributes.'gen_ai.usage.output_tokens'::Int64)) AS output_tokens
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY model
		ORDER BY (input_tokens + output_tokens) DESC
	`, args...)
	return rows, err
}

// ---- cost timeseries ----

func (r *ClickHouseRepository) GetCostTimeseries(ctx context.Context, f AnalyticsFilter) ([]costTimeseriesRow, error) {
	extra, args := allArgs(f)
	bucket := timebucket.ExprForColumnTime(f.StartMs, f.EndMs, "s.timestamp")
	var rows []costTimeseriesRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s AS timestamp,
		       attributes.'gen_ai.request.model'::String                    AS model,
		       toInt64(sum(attributes.'gen_ai.usage.input_tokens'::Int64))  AS input_tokens,
		       toInt64(sum(attributes.'gen_ai.usage.output_tokens'::Int64)) AS output_tokens
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY timestamp, model
		ORDER BY timestamp ASC
	`, bucket), args...)
	return rows, err
}

// ---- token economics ----

func (r *ClickHouseRepository) GetTokenEconomics(ctx context.Context, f AnalyticsFilter) (tokenEconomicsRow, error) {
	extra, args := allArgs(f)
	var rows []tokenEconomicsRow
	err := r.db.Select(ctx, &rows, `
		SELECT toInt64(sum(attributes.'gen_ai.usage.input_tokens'::Int64))                              AS total_input,
		       toInt64(sum(attributes.'gen_ai.usage.output_tokens'::Int64))                             AS total_output,
		       toFloat64(sum(attributes.'gen_ai.usage.input_tokens'::Int64) + sum(attributes.'gen_ai.usage.output_tokens'::Int64)) / greatest(count(), 1) AS avg_per_req,
		       toInt64(count())                                                                          AS request_count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra, args...)
	if err != nil || len(rows) == 0 {
		return tokenEconomicsRow{}, err
	}
	return rows[0], nil
}

// ---- error patterns ----

func (r *ClickHouseRepository) GetErrorPatterns(ctx context.Context, f AnalyticsFilter, limit int) ([]errorPatternRow, error) {
	extra, args := allArgs(f)
	args = append(args, clickhouse.Named("limit", limit))
	var rows []errorPatternRow
	err := r.db.Select(ctx, &rows, `
		SELECT attributes.'gen_ai.request.model'::String AS model,
		       attributes.'gen_ai.operation.name'::String AS operation,
		       status_message,
		       toInt64(count()) AS error_count,
		       min(timestamp) AS first_seen,
		       max(timestamp) AS last_seen
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		  AND s.has_error = true
		GROUP BY model, operation, status_message
		ORDER BY error_count DESC
		LIMIT @limit
	`, args...)
	return rows, err
}

// ---- error timeseries ----

func (r *ClickHouseRepository) GetErrorTimeseries(ctx context.Context, f AnalyticsFilter) ([]errorTimeseriesRow, error) {
	extra, args := allArgs(f)
	bucket := timebucket.ExprForColumnTime(f.StartMs, f.EndMs, "s.timestamp")
	var rows []errorTimeseriesRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s AS timestamp,
		       attributes.'gen_ai.request.model'::String AS model,
		       toInt64(countIf(has_error = true)) AS error_count,
		       countIf(has_error = true) * 100.0 / count() AS error_rate
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		GROUP BY timestamp, model
		ORDER BY timestamp ASC
	`, bucket), args...)
	return rows, err
}

// ---- finish reason trends ----

func (r *ClickHouseRepository) GetFinishReasonTrends(ctx context.Context, f AnalyticsFilter) ([]finishReasonTrendRow, error) {
	extra, args := allArgs(f)
	bucket := timebucket.ExprForColumnTime(f.StartMs, f.EndMs, "s.timestamp")
	var rows []finishReasonTrendRow
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s AS timestamp,
		       attributes.'gen_ai.response.finish_reasons'::String AS finish_reason,
		       toInt64(count()) AS count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		  AND finish_reason != ''
		GROUP BY timestamp, finish_reason
		ORDER BY timestamp ASC
	`, bucket), args...)
	return rows, err
}

// ---- conversations ----

func (r *ClickHouseRepository) GetConversations(ctx context.Context, f AnalyticsFilter, limit, offset int) ([]conversationListRow, error) {
	extra, args := allArgs(f)
	args = append(args, clickhouse.Named("limit", limit), clickhouse.Named("offset", offset))
	var rows []conversationListRow
	err := r.db.Select(ctx, &rows, `
		SELECT attributes.'gen_ai.conversation.id'::String AS conversation_id,
		       any(service_name) AS service_name,
		       any(attributes.'gen_ai.request.model'::String) AS model,
		       toInt64(count()) AS turn_count,
		       toInt64(sum(attributes.'gen_ai.usage.input_tokens'::Int64 + attributes.'gen_ai.usage.output_tokens'::Int64)) AS total_tokens,
		       max(has_error) AS has_error,
		       min(timestamp) AS first_turn,
		       max(timestamp) AS last_turn
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND `+genAIFilter+extra+`
		  AND conversation_id != ''
		GROUP BY conversation_id
		ORDER BY last_turn DESC
		LIMIT @limit OFFSET @offset
	`, args...)
	return rows, err
}

func (r *ClickHouseRepository) GetConversationTurns(ctx context.Context, teamID int64, conversationID string) ([]conversationTurnRow, error) {
	var rows []conversationTurnRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id                                          AS span_id,
		       s.timestamp                                        AS timestamp,
		       attributes.'gen_ai.request.model'::String          AS model,
		       duration_nano / 1000000.0                          AS duration_ms,
		       attributes.'gen_ai.usage.input_tokens'::Int64      AS input_tokens,
		       attributes.'gen_ai.usage.output_tokens'::Int64     AS output_tokens,
		       s.has_error                                        AS has_error,
		       attributes.'gen_ai.response.finish_reasons'::String AS finish_reason
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND attributes.'gen_ai.conversation.id'::String = @conversationID
		ORDER BY s.timestamp ASC
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("conversationID", conversationID),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetConversationSummary(ctx context.Context, teamID int64, conversationID string) (conversationSummaryRow, error) {
	var rows []conversationSummaryRow
	err := r.db.Select(ctx, &rows, `
		SELECT toInt64(count()) AS turn_count,
		       toInt64(sum(attributes.'gen_ai.usage.input_tokens'::Int64 + attributes.'gen_ai.usage.output_tokens'::Int64)) AS total_tokens,
		       sum(duration_nano / 1000000.0) AS total_ms,
		       arrayStringConcat(groupUniqArray(attributes.'gen_ai.request.model'::String), ', ') AS models,
		       toInt64(countIf(has_error = true)) AS error_turns,
		       min(timestamp) AS first_turn,
		       max(timestamp) AS last_turn
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND attributes.'gen_ai.conversation.id'::String = @conversationID
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("conversationID", conversationID),
	)
	if err != nil || len(rows) == 0 {
		return conversationSummaryRow{}, err
	}
	return rows[0], nil
}
