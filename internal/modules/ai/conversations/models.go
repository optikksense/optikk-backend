package conversations

import "time"

// Conversation represents a grouped multi-turn LLM conversation.
type Conversation struct {
	ConversationID string    `ch:"conversation_id" json:"conversationId"`
	ServiceName    string    `ch:"service_name"    json:"serviceName"`
	Model          string    `ch:"model"           json:"model"`
	TurnCount      int64     `ch:"turn_count"      json:"turnCount"`
	TotalTokens    int64     `ch:"total_tokens"    json:"totalTokens"`
	FirstTurn      time.Time `ch:"first_turn"      json:"firstTurn"`
	LastTurn       time.Time `ch:"last_turn"       json:"lastTurn"`
	HasErrors      bool      `ch:"has_errors"      json:"hasErrors"`
}

// ConversationTurn represents a single turn within a conversation.
type ConversationTurn struct {
	SpanID        string    `ch:"span_id"        json:"spanId"`
	TraceID       string    `ch:"trace_id"       json:"traceId"`
	Model         string    `ch:"model"          json:"model"`
	OperationType string    `ch:"operation_type" json:"operationType,omitempty"`
	StartTime     time.Time `ch:"timestamp"      json:"startTime"`
	DurationMs    float64   `ch:"duration_ms"    json:"durationMs"`
	InputTokens   int64     `ch:"input_tokens"   json:"inputTokens"`
	OutputTokens  int64     `ch:"output_tokens"  json:"outputTokens"`
	HasError      bool      `ch:"has_error"      json:"hasError"`
	InputPreview  string    `json:"inputPreview,omitempty"`
	OutputPreview string    `json:"outputPreview,omitempty"`
}
