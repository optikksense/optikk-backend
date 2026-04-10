package spandetail

import "time"

// -------- DB scan models --------

type spanDetailRow struct {
	SpanID           string    `ch:"span_id"`
	TraceID          string    `ch:"trace_id"`
	ParentSpanID     string    `ch:"parent_span_id"`
	ServiceName      string    `ch:"service_name"`
	OperationName    string    `ch:"operation_name"`
	KindString       string    `ch:"kind_string"`
	Timestamp        time.Time `ch:"timestamp"`
	DurationMs       float64   `ch:"duration_ms"`
	HasError         bool      `ch:"has_error"`
	StatusMessage    string    `ch:"status_message"`
	Model            string    `ch:"model"`
	ResponseModel    string    `ch:"response_model"`
	Provider         string    `ch:"provider"`
	OperationType    string    `ch:"operation_type"`
	Temperature      float64   `ch:"temperature"`
	TopP             float64   `ch:"top_p"`
	MaxTokens        int64     `ch:"max_tokens"`
	FrequencyPenalty float64   `ch:"frequency_penalty"`
	PresencePenalty  float64   `ch:"presence_penalty"`
	Seed             int64     `ch:"seed"`
	InputTokens      int64     `ch:"input_tokens"`
	OutputTokens     int64     `ch:"output_tokens"`
	FinishReason     string    `ch:"finish_reason"`
	ResponseID       string    `ch:"response_id"`
	ServerAddress    string    `ch:"server_address"`
	ConversationID   string    `ch:"conversation_id"`
}

type traceSpanRow struct {
	SpanID        string    `ch:"span_id"`
	ParentSpanID  string    `ch:"parent_span_id"`
	ServiceName   string    `ch:"service_name"`
	OperationName string    `ch:"operation_name"`
	KindString    string    `ch:"kind_string"`
	Timestamp     time.Time `ch:"timestamp"`
	DurationMs    float64   `ch:"duration_ms"`
	HasError      bool      `ch:"has_error"`
	IsAI          bool      `ch:"is_ai"`
}

type relatedSpanRow struct {
	SpanID       string    `ch:"span_id"`
	Timestamp    time.Time `ch:"timestamp"`
	DurationMs   float64   `ch:"duration_ms"`
	InputTokens  int64     `ch:"input_tokens"`
	OutputTokens int64     `ch:"output_tokens"`
	HasError     bool      `ch:"has_error"`
	FinishReason string    `ch:"finish_reason"`
}

type tokenBreakdownRow struct {
	InputTokens    int64   `ch:"input_tokens"`
	OutputTokens   int64   `ch:"output_tokens"`
	AvgInputModel  float64 `ch:"avg_input_model"`
	AvgOutputModel float64 `ch:"avg_output_model"`
}

type messageRow struct {
	EventName string `ch:"event_name"`
	Body      string `ch:"body"`
}
