package errors

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

type ErrorGroup struct {
	GroupID         string    `json:"group_id"`
	ServiceName     string    `json:"service_name"`
	OperationName   string    `json:"operation_name"`
	StatusMessage   string    `json:"status_message"`
	HTTPStatusCode  int       `json:"http_status_code"`
	ErrorCount      int64     `json:"error_count"`
	LastOccurrence  time.Time `json:"last_occurrence"`
	FirstOccurrence time.Time `json:"first_occurrence"`
	SampleTraceID   string    `json:"sample_trace_id"`
}

// ErrorGroupID computes a deterministic hash from the group's identity fields.
func ErrorGroupID(service, operation, statusMessage string, httpCode int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d", service, operation, statusMessage, httpCode)))
	return hex.EncodeToString(h[:8])
}

type ErrorGroupDetail struct {
	GroupID         string    `json:"group_id"`
	ServiceName     string    `json:"service_name"`
	OperationName   string    `json:"operation_name"`
	StatusMessage   string    `json:"status_message"`
	HTTPStatusCode  int       `json:"http_status_code"`
	ErrorCount      int64     `json:"error_count"`
	LastOccurrence  time.Time `json:"last_occurrence"`
	FirstOccurrence time.Time `json:"first_occurrence"`
	SampleTraceID   string    `json:"sample_trace_id"`
	StackTrace      string    `json:"stack_trace,omitempty"`
	ExceptionType   string    `json:"exception_type,omitempty"`
}

type ErrorGroupTrace struct {
	TraceID    string    `json:"trace_id"`
	SpanID     string    `json:"span_id"`
	Timestamp  time.Time `json:"timestamp"`
	DurationMs float64   `json:"duration_ms"`
	StatusCode string    `json:"status_code"`
}

type TimeSeriesPoint struct {
	ServiceName  string    `json:"service_name"`
	Timestamp    time.Time `json:"timestamp"`
	RequestCount int64     `json:"request_count"`
	ErrorCount   int64     `json:"error_count"`
	ErrorRate    float64   `json:"error_rate"`
	AvgLatency   float64   `json:"avg_latency"`
}
