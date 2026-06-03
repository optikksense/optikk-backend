package servicemap

import (
	"time"
)

type TraceErrorGroup struct {
	ExceptionType string           `json:"exception_type"`
	Count         int              `json:"count"`
	Spans         []TraceErrorSpan `json:"spans"`
}

type TraceErrorSpan struct {
	SpanID           string    `json:"span_id"`
	ServiceName      string    `json:"service_name"`
	OperationName    string    `json:"operation_name"`
	ExceptionMessage string    `json:"exception_message,omitempty"`
	StatusMessage    string    `json:"status_message,omitempty"`
	StartTime        time.Time `json:"start_time"`
	DurationMs       float64   `json:"duration_ms"`
}
