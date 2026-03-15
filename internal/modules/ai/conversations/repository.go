package conversations

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

const (
	tableSpans         = "observability.spans"
	colModel           = "attributes.'gen_ai.request.model'::String"
	colConversationID  = "attributes.'gen_ai.conversation.id'::String"
	colOperationType   = "attributes.'gen_ai.operation.name'::String"
	colInputTokens     = "attributes.'gen_ai.usage.input_tokens'::Int64"
	colOutputTokens    = "attributes.'gen_ai.usage.output_tokens'::Int64"
)

type Repository interface {
	ListConversations(teamID, startMs, endMs int64, limit int) ([]Conversation, error)
	GetConversation(teamID int64, conversationID string, startMs, endMs int64) ([]ConversationTurn, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) ListConversations(teamID, startMs, endMs int64, limit int) ([]Conversation, error) {
	if limit <= 0 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT %s AS conversation_id,
		       s.service_name,
		       %s AS model,
		       COUNT(*) AS turn_count,
		       SUM(%s + %s) AS total_tokens,
		       MIN(s.timestamp) AS first_turn,
		       MAX(s.timestamp) AS last_turn,
		       max(s.has_error) AS has_errors
		FROM %s s
		WHERE s.team_id = ?
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?
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
		uint32(teamID),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		dbutil.SqlTime(startMs),
		dbutil.SqlTime(endMs),
	}

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	convos := make([]Conversation, 0, len(rows))
	for _, row := range rows {
		convos = append(convos, Conversation{
			ConversationID: dbutil.StringFromAny(row["conversation_id"]),
			ServiceName:    dbutil.StringFromAny(row["service_name"]),
			Model:          dbutil.StringFromAny(row["model"]),
			TurnCount:      dbutil.Int64FromAny(row["turn_count"]),
			TotalTokens:    dbutil.Int64FromAny(row["total_tokens"]),
			FirstTurn:      dbutil.TimeFromAny(row["first_turn"]),
			LastTurn:       dbutil.TimeFromAny(row["last_turn"]),
			HasErrors:      dbutil.BoolFromAny(row["has_errors"]),
		})
	}
	return convos, nil
}

func (r *ClickHouseRepository) GetConversation(teamID int64, conversationID string, startMs, endMs int64) ([]ConversationTurn, error) {
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
		WHERE s.team_id = ?
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?
		  AND %s = ?
		ORDER BY s.timestamp ASC
		LIMIT 500
	`, colModel, colOperationType,
		colInputTokens, colOutputTokens,
		tableSpans, colConversationID)

	args := []any{
		uint32(teamID),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		dbutil.SqlTime(startMs),
		dbutil.SqlTime(endMs),
		conversationID,
	}

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	turns := make([]ConversationTurn, 0, len(rows))
	for _, row := range rows {
		turns = append(turns, ConversationTurn{
			SpanID:        dbutil.StringFromAny(row["span_id"]),
			TraceID:       dbutil.StringFromAny(row["trace_id"]),
			Model:         dbutil.StringFromAny(row["model"]),
			OperationType: dbutil.StringFromAny(row["operation_type"]),
			StartTime:     dbutil.TimeFromAny(row["timestamp"]),
			DurationMs:    dbutil.Float64FromAny(row["duration_ms"]),
			InputTokens:   dbutil.Int64FromAny(row["input_tokens"]),
			OutputTokens:  dbutil.Int64FromAny(row["output_tokens"]),
			HasError:      dbutil.BoolFromAny(row["has_error"]),
		})
	}
	return turns, nil
}
