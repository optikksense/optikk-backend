package livetail

import "time"

// LiveSpan is a simplified span emitted over the SSE stream.
type LiveSpan struct {
	SpanID        string    `json:"spanId"`
	TraceID       string    `json:"traceId"`
	ServiceName   string    `json:"serviceName"`
	OperationName string    `json:"operationName"`
	DurationMs    float64   `json:"durationMs"`
	Status        string    `json:"status"`
	HTTPMethod    string    `json:"httpMethod,omitempty"`
	HTTPStatus    string    `json:"httpStatusCode,omitempty"`
	SpanKind      string    `json:"spanKind,omitempty"`
	HasError      bool      `json:"hasError"`
	Timestamp     time.Time `json:"timestamp"`
}

// LiveTailFilters are the optional filters applied to the live tail stream.
type LiveTailFilters struct {
	Services []string `json:"services,omitempty"`
	Status   string   `json:"status,omitempty"`
	SpanKind string   `json:"spanKind,omitempty"`
}
