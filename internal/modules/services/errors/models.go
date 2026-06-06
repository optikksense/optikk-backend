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
	HTTPStatusCode  int       `json:"http_status_code"`
	ErrorCount      int64     `json:"error_count"`
	LastOccurrence  time.Time `json:"last_occurrence"`
	FirstOccurrence time.Time `json:"first_occurrence"`
	ExceptionType   string    `json:"exception_type,omitempty"`
}

type ErrorGroupTrace struct {
	TraceID    string    `json:"trace_id"`
	SpanID     string    `json:"span_id"`
	Timestamp  time.Time `json:"timestamp"`
	DurationMs float64   `json:"duration_ms"`
	StatusCode string    `json:"status_code"`
}

type PaginatedErrorTraces struct {
	Results  []ErrorGroupTrace `json:"results"`
	PageInfo PageInfo          `json:"pageInfo"`
}

// ErrorLatestOccurrence is the context of a group's most recent error span.
type ErrorLatestOccurrence struct {
	TraceID        string    `json:"trace_id"`
	SpanID         string    `json:"span_id"`
	Timestamp      time.Time `json:"timestamp"`
	DurationMs     float64   `json:"duration_ms"`
	Message        string    `json:"message"`
	Stacktrace     string    `json:"stacktrace,omitempty"`
	HTTPMethod     string    `json:"http_method"`
	HTTPRoute      string    `json:"http_route"`
	HTTPStatusCode string    `json:"http_status_code"`
	ServiceVersion string    `json:"service_version"`
	Environment    string    `json:"environment"`
	Pod            string    `json:"pod"`
	Host           string    `json:"host"`
}

// ErrorFacet is one value within a facet dimension with its error count share.
type ErrorFacet struct {
	Name  string  `json:"name"`
	Count int64   `json:"count"`
	Pct   float64 `json:"pct"`
}

// ErrorFacetGroup is the distribution of group errors across one tag dimension.
type ErrorFacetGroup struct {
	Key    string       `json:"key"`
	Facets []ErrorFacet `json:"facets"`
}

type TimeSeriesPoint struct {
	ServiceName  string    `json:"service_name"`
	Timestamp    time.Time `json:"timestamp"`
	RequestCount int64     `json:"request_count"`
	ErrorCount   int64     `json:"error_count"`
	ErrorRate    float64   `json:"error_rate"`
	AvgLatency   float64   `json:"avg_latency"`
}

// ErrorHotspotCell imported from errortracking
type ErrorHotspotCell struct {
	ServiceName   string  `json:"service_name"   ch:"service"`
	OperationName string  `json:"operation_name" ch:"operation_name"`
	GroupID       string  `json:"group_id"       ch:"error_group_id"`
	ErrorRate     float64 `json:"error_rate"     ch:"error_rate"`
	ErrorCount    int64   `json:"error_count"    ch:"error_count"`
	TotalCount    int64   `json:"total_count"    ch:"total_count"`
}
