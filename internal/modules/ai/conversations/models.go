package conversations

import "time"

// Conversation represents a grouped multi-turn LLM conversation.
type Conversation struct {
	ConversationID string    `json:"conversationId"`
	ServiceName    string    `json:"serviceName"`
	Model          string    `json:"model"`
	TurnCount      int64     `json:"turnCount"`
	TotalTokens    int64     `json:"totalTokens"`
	FirstTurn      time.Time `json:"firstTurn"`
	LastTurn       time.Time `json:"lastTurn"`
	HasErrors      bool      `json:"hasErrors"`
}

// ConversationTurn represents a single turn within a conversation.
type ConversationTurn struct {
	SpanID        string    `json:"spanId"`
	TraceID       string    `json:"traceId"`
	Model         string    `json:"model"`
	OperationType string    `json:"operationType,omitempty"`
	StartTime     time.Time `json:"startTime"`
	DurationMs    float64   `json:"durationMs"`
	InputTokens   int64     `json:"inputTokens"`
	OutputTokens  int64     `json:"outputTokens"`
	HasError      bool      `json:"hasError"`
	InputPreview  string    `json:"inputPreview,omitempty"`
	OutputPreview string    `json:"outputPreview,omitempty"`
}
