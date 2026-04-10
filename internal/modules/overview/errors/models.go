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

// ExceptionRatePoint imported from errortracking
type ExceptionRatePoint struct {
	Timestamp     time.Time `json:"timestamp"      ch:"time_bucket"`
	ExceptionType string    `json:"exception_type" ch:"exception_type"`
	Count         int64     `json:"count"          ch:"event_count"`
}

// ErrorHotspotCell imported from errortracking
type ErrorHotspotCell struct {
	ServiceName   string  `json:"service_name"   ch:"service_name"`
	OperationName string  `json:"operation_name" ch:"operation_name"`
	ErrorRate     float64 `json:"error_rate"     ch:"error_rate"`
	ErrorCount    int64   `json:"error_count"    ch:"error_count"`
	TotalCount    int64   `json:"total_count"    ch:"total_count"`
}

// HTTP5xxByRoute imported from errortracking
type HTTP5xxByRoute struct {
	HTTPRoute   string `json:"http_route"   ch:"http_route"`
	ServiceName string `json:"service_name" ch:"service_name"`
	Count       int64  `json:"count"        ch:"count_5xx"`
}

// ErrorFingerprint imported from errorfingerprint
type ErrorFingerprint struct {
	Fingerprint   string    `json:"fingerprint"  ch:"fingerprint"`
	ServiceName   string    `json:"serviceName"  ch:"service_name"`
	OperationName string    `json:"operationName" ch:"operation_name"`
	ExceptionType string    `json:"exceptionType" ch:"exception_type"`
	StatusMessage string    `json:"statusMessage" ch:"status_message"`
	FirstSeen     time.Time `json:"firstSeen"    ch:"first_seen"`
	LastSeen      time.Time `json:"lastSeen"     ch:"last_seen"`
	Count         int64     `json:"count"        ch:"cnt"`
	SampleTraceID string    `json:"sampleTraceId" ch:"sample_trace_id"`
}

// FingerprintTrendPoint imported from errorfingerprint
type FingerprintTrendPoint struct {
	Timestamp time.Time `json:"timestamp" ch:"ts"`
	Count     int64     `json:"count"     ch:"cnt"`
}
