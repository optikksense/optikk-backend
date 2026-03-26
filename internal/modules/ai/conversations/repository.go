package conversations

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

const (
	tableSpans        = "observability.spans"
	colModel          = "attributes.'gen_ai.request.model'::String"
	colConversationID = "coalesce(nullIf(attributes.'gen_ai.conversation.id'::String, ''), nullIf(attributes.'ai.conversation.id'::String, ''), '')"
	colOperationType  = "attributes.'gen_ai.operation.name'::String"
	colInputTokens    = "attributes.'gen_ai.usage.input_tokens'::Int64"  //nolint:gosec // G101 - column expressions, not credentials
	colOutputTokens   = "attributes.'gen_ai.usage.output_tokens'::Int64" //nolint:gosec // G101 - column expressions, not credentials
)

type Repository interface {
	ListConversations(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]conversationRowDTO, error)
	GetConversation(ctx context.Context, teamID int64, conversationID string, startMs, endMs int64) ([]conversationTurnRowDTO, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) ListConversations(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]conversationRowDTO, error) {
	if limit <= 0 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT %s AS conversation_id,
		       s.service_name,
		       %s AS model,
		       toInt64(COUNT(*)) AS turn_count,
		       toInt64(COALESCE(SUM(%s + %s), 0)) AS total_tokens,
		       MIN(s.timestamp) AS first_turn,
		       MAX(s.timestamp) AS last_turn,
		       max(s.has_error) AS has_errors
		FROM %s s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND %s != ''
		  AND %s != ''
		GROUP BY conversation_id, s.service_name, model
		ORDER BY last_turn DESC
		LIMIT %d
	`, colConversationID, colModel,
		colInputTokens, colOutputTokens,
		tableSpans,
		colConversationID, colModel,
		limit)

	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var rows []conversationRowDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetConversation(ctx context.Context, teamID int64, conversationID string, startMs, endMs int64) ([]conversationTurnRowDTO, error) {
	query := fmt.Sprintf(`
		SELECT s.span_id, s.trace_id,
		       %s AS model,
		       %s AS operation_type,
		       s.timestamp,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       %s AS input_tokens,
		       %s AS output_tokens,
		       s.has_error
		FROM %s s
		WHERE s.team_id = @teamID
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND %s = @conversationID
		ORDER BY s.timestamp ASC
		LIMIT 500
	`, colModel, colOperationType,
		colInputTokens, colOutputTokens,
		tableSpans, colConversationID)

	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("conversationID", conversationID),
	}

	var rows []conversationTurnRowDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}
