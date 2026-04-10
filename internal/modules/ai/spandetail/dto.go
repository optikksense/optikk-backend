package spandetail

import "time"

// -------- API response DTOs --------

type SpanDetailDTO struct {
	SpanID           string    `json:"spanId"`
	TraceID          string    `json:"traceId"`
	ParentSpanID     string    `json:"parentSpanId"`
	ServiceName      string    `json:"serviceName"`
	OperationName    string    `json:"operationName"`
	KindString       string    `json:"kindString"`
	Timestamp        time.Time `json:"timestamp"`
	DurationMs       float64   `json:"durationMs"`
	HasError         bool      `json:"hasError"`
	StatusMessage    string    `json:"statusMessage"`
	Model            string    `json:"model"`
	ResponseModel    string    `json:"responseModel"`
	Provider         string    `json:"provider"`
	OperationType    string    `json:"operationType"`
	Temperature      float64   `json:"temperature"`
	TopP             float64   `json:"topP"`
	MaxTokens        int64     `json:"maxTokens"`
	FrequencyPenalty float64   `json:"frequencyPenalty"`
	PresencePenalty  float64   `json:"presencePenalty"`
	Seed             int64     `json:"seed"`
	InputTokens      int64     `json:"inputTokens"`
	OutputTokens     int64     `json:"outputTokens"`
	TotalTokens      int64     `json:"totalTokens"`
	TokensPerSec     float64   `json:"tokensPerSec"`
	FinishReason     string    `json:"finishReason"`
	ResponseID       string    `json:"responseId"`
	ServerAddress    string    `json:"serverAddress"`
	ConversationID   string    `json:"conversationId"`
}

type MessageDTO struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type TraceContextSpanDTO struct {
	SpanID        string    `json:"spanId"`
	ParentSpanID  string    `json:"parentSpanId"`
	ServiceName   string    `json:"serviceName"`
	OperationName string    `json:"operationName"`
	KindString    string    `json:"kindString"`
	Timestamp     time.Time `json:"timestamp"`
	DurationMs    float64   `json:"durationMs"`
	HasError      bool      `json:"hasError"`
	IsAI          bool      `json:"isAi"`
}

type RelatedSpanDTO struct {
	SpanID       string    `json:"spanId"`
	Timestamp    time.Time `json:"timestamp"`
	DurationMs   float64   `json:"durationMs"`
	InputTokens  int64     `json:"inputTokens"`
	OutputTokens int64     `json:"outputTokens"`
	TotalTokens  int64     `json:"totalTokens"`
	HasError     bool      `json:"hasError"`
	FinishReason string    `json:"finishReason"`
}

type TokenBreakdownDTO struct {
	InputTokens    int64   `json:"inputTokens"`
	OutputTokens   int64   `json:"outputTokens"`
	TotalTokens    int64   `json:"totalTokens"`
	AvgInputModel  float64 `json:"avgInputModel"`
	AvgOutputModel float64 `json:"avgOutputModel"`
}
