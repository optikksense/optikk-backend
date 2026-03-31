package livetail

import "time"

// LiveSpan is a simplified span emitted to clients (e.g. Socket.IO live tail).
type LiveSpan struct {
	SpanID        string    `json:"spanId"               ch:"span_id"`
	TraceID       string    `json:"traceId"              ch:"trace_id"`
	ServiceName   string    `json:"serviceName"          ch:"service_name"`
	OperationName string    `json:"operationName"        ch:"operation_name"`
	DurationMs    float64   `json:"durationMs"           ch:"duration_ms"`
	Status        string    `json:"status"               ch:"status"`
	HTTPMethod    string    `json:"httpMethod,omitempty" ch:"http_method"`
	HTTPStatus    string    `json:"httpStatusCode,omitempty" ch:"http_status_code"`
	SpanKind      string    `json:"spanKind,omitempty"   ch:"span_kind"`
	HasError      bool      `json:"hasError"             ch:"has_error"`
	Timestamp     time.Time `json:"timestamp"            ch:"timestamp"`
}

// LiveTailFilters are the optional filters applied to the live tail stream.
type LiveTailFilters struct {
	Services   []string `json:"services,omitempty"`
	Status     string   `json:"status,omitempty"`
	SpanKind   string   `json:"spanKind,omitempty"`
	SearchText string   `json:"searchText,omitempty"`
	Operation  string   `json:"operation,omitempty"`
	HTTPMethod string   `json:"httpMethod,omitempty"`
}

type PollResult struct {
	Spans        []LiveSpan `json:"spans"`
	DroppedCount int64      `json:"droppedCount"`
}
