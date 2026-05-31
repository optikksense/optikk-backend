package traces

import (
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

type spanEventRow struct {
	SpanID    string    `ch:"span_id"`
	TraceID   string    `ch:"trace_id"`
	Timestamp time.Time `ch:"timestamp"`
	EventJSON string    `ch:"event_json"`
}

type exceptionRow struct {
	SpanID              string    `ch:"span_id"`
	TraceID             string    `ch:"trace_id"`
	Timestamp           time.Time `ch:"timestamp"`
	ExceptionType       string    `ch:"exception_type"`
	ExceptionMessage    string    `ch:"exception_message"`
	ExceptionStacktrace string    `ch:"exception_stacktrace"`
}

type spanEventCombinedRow struct {
	SpanID              string    `ch:"span_id"`
	TraceID             string    `ch:"trace_id"`
	Timestamp           time.Time `ch:"timestamp"`
	Events              []string  `ch:"events"`
	ExceptionType       string    `ch:"exception_type"`
	ExceptionMessage    string    `ch:"exception_message"`
	ExceptionStacktrace string    `ch:"exception_stacktrace"`
}

type spanAttributeRow struct {
	SpanID              string `ch:"span_id"`
	TraceID             string `ch:"trace_id"`
	OperationName       string `ch:"operation_name"`
	ServiceName         string `ch:"service"`
	AttributesJSON      string `ch:"attributes_json"`
	ExceptionType       string `ch:"exception_type"`
	ExceptionMessage    string `ch:"exception_message"`
	ExceptionStacktrace string `ch:"exception_stacktrace"`
	DBSystem            string `ch:"db_system"`
	DBName              string `ch:"db_name"`
	DBStatement         string `ch:"db_statement"`
	Links               string `ch:"links"`
}

type QueryRequest struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Limit     int    `json:"limit"`
	Cursor    string `json:"cursor"`

	filter.Filters
}

type QueryResponse struct {
	Results  []Trace  `json:"results"`
	PageInfo PageInfo `json:"pageInfo"`
}

type traceIndexRowDTO struct {
	TraceID        string    `ch:"trace_id"`
	StartTime      time.Time `ch:"start_time"`
	EndTime        time.Time `ch:"end_time"`
	DurationNs     uint64    `ch:"duration_ns"`
	RootService    string    `ch:"root_service"`
	RootOperation  string    `ch:"root_operation"`
	RootStatus     string    `ch:"root_status"`
	RootHTTPMethod string    `ch:"root_http_method"`
	RootHTTPStatus string    `ch:"root_http_status"`
	SpanCount      uint8     `ch:"span_count"`
	HasError       bool      `ch:"has_error"`
	ErrorCount     uint8     `ch:"error_count"`
	ServiceSet     []string  `ch:"service_set"`
	Truncated      bool      `ch:"truncated"`
	LastSeen       time.Time `ch:"last_seen"`
}

type FacetsRequest struct {
	StartTime int64 `json:"startTime"`
	EndTime   int64 `json:"endTime"`

	filter.Filters
}

type topKRow struct {
	TopServices     []string `ch:"top_services"`
	TopOperations   []string `ch:"top_operations"`
	TopHTTPMethods  []string `ch:"top_http_methods"`
	TopHTTPStatuses []string `ch:"top_http_statuses"`
	TopStatuses     []string `ch:"top_statuses"`
}

type TrendRequest struct {
	StartTime int64 `json:"startTime"`
	EndTime   int64 `json:"endTime"`

	filter.Filters
}

type SuggestRequest struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Field     string `json:"field"`
	Prefix    string `json:"prefix"`
	Limit     int    `json:"limit"`
}

type suggestionRow struct {
	Value string `ch:"value"`
	Count uint64 `ch:"count"`
}

type criticalPathRow struct {
	SpanID        string    `ch:"span_id"`
	ParentSpanID  string    `ch:"parent_span_id"`
	OperationName string    `ch:"operation_name"`
	ServiceName   string    `ch:"service"`
	DurationMs    float64   `ch:"duration_ms"`
	Timestamp     time.Time `ch:"timestamp"`
	DurationNano  uint64    `ch:"duration_nano"`
}

type errorPathRow struct {
	SpanID        string    `ch:"span_id"`
	ParentSpanID  string    `ch:"parent_span_id"`
	OperationName string    `ch:"operation_name"`
	ServiceName   string    `ch:"service"`
	Status        string    `ch:"status"`
	StatusMessage string    `ch:"status_message"`
	StartTime     time.Time `ch:"start_time"`
	DurationMs    float64   `ch:"duration_ms"`
}

type serviceMapSpanRow struct {
	SpanID       string  `ch:"span_id"`
	ParentSpanID string  `ch:"parent_span_id"`
	ServiceName  string  `ch:"service"`
	DurationMs   float64 `ch:"duration_ms"`
	HasError     bool    `ch:"has_error"`
}

type traceErrorRow struct {
	SpanID           string    `ch:"span_id"`
	ServiceName      string    `ch:"service"`
	OperationName    string    `ch:"operation_name"`
	ExceptionType    string    `ch:"exception_type"`
	ExceptionMessage string    `ch:"exception_message"`
	StatusMessage    string    `ch:"status_message"`
	StartTime        time.Time `ch:"start_time"`
	DurationMs       float64   `ch:"duration_ms"`
}
