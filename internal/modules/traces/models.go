package traces

import (
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/cursor"
)

// TraceSummary is the per-trace summary card returned by GET /traces/:traceId.
type TraceSummary struct {
	TraceID        string   `json:"trace_id"`
	StartMs        uint64   `json:"start_ms"`
	EndMs          uint64   `json:"end_ms"`
	DurationMs     float64  `json:"duration_ms"`
	RootService    string   `json:"root_service"`
	RootOperation  string   `json:"root_operation"`
	RootStatus     string   `json:"root_status,omitempty"`
	RootHTTPMethod string   `json:"root_http_method,omitempty"`
	RootHTTPStatus string   `json:"root_http_status,omitempty"`
	SpanCount      uint32   `json:"span_count"`
	HasError       bool     `json:"has_error"`
	ErrorCount     uint32   `json:"error_count"`
	ServiceSet     []string `json:"service_set,omitempty"`
	Truncated      bool     `json:"truncated,omitempty"`
}

type SpanEvent struct {
	SpanID     string    `json:"span_id"     ch:"span_id"`
	TraceID    string    `json:"trace_id"    ch:"trace_id"`
	EventName  string    `json:"event_name"  ch:"event_name"`
	Timestamp  time.Time `json:"timestamp"   ch:"timestamp"`
	Attributes string    `json:"attributes"`
}

type SpanAttributes struct {
	SpanID                string            `json:"span_id"`
	TraceID               string            `json:"trace_id"`
	OperationName         string            `json:"operation_name"`
	ServiceName           string            `json:"service_name"`
	AttributesString      map[string]string `json:"attributes_string"`
	ResourceAttrs         map[string]string `json:"resource_attributes"`
	ExceptionType         string            `json:"exception_type,omitempty"`
	ExceptionMessage      string            `json:"exception_message,omitempty"`
	ExceptionStacktrace   string            `json:"exception_stacktrace,omitempty"`
	DBSystem              string            `json:"db_system,omitempty"`
	DBName                string            `json:"db_name,omitempty"`
	DBStatement           string            `json:"db_statement,omitempty"`
	DBStatementNormalized string            `json:"db_statement_normalized,omitempty"`
	Attributes            map[string]string `json:"attributes,omitempty"`
	Links                 []SpanLink        `json:"links,omitempty"`
}

type SpanLink struct {
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	TraceState string            `json:"trace_state,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type RelatedTrace struct {
	TraceID       string    `json:"trace_id"       ch:"trace_id"`
	SpanID        string    `json:"span_id"        ch:"span_id"`
	OperationName string    `json:"operation_name" ch:"operation_name"`
	ServiceName   string    `json:"service_name"   ch:"service"`
	DurationMs    float64   `json:"duration_ms"    ch:"duration_ms"`
	Status        string    `json:"status"         ch:"status"`
	StartTime     time.Time `json:"start_time"     ch:"start_time"`
}

type SpanListItem struct {
	SpanID        string    `json:"span_id"        ch:"span_id"`
	ParentSpanID  string    `json:"parent_span_id" ch:"parent_span_id"`
	TraceID       string    `json:"trace_id"       ch:"trace_id"`
	ServiceName   string    `json:"service_name"   ch:"service"`
	OperationName string    `json:"operation_name" ch:"name"`
	KindString    string    `json:"kind"           ch:"kind_string"`
	StatusCode    string    `json:"status_code"    ch:"status_code_string"`
	HasError      bool      `json:"has_error"      ch:"has_error"`
	DurationMs    float64   `json:"duration_ms"    ch:"duration_ms"`
	Timestamp     time.Time `json:"-"              ch:"timestamp"`
	StartNs       int64     `json:"start_ns"       ch:"-"`
}

type Trace struct {
	TraceID        string   `json:"trace_id"`
	StartMs        uint64   `json:"start_ms"`
	EndMs          uint64   `json:"end_ms"`
	DurationMs     float64  `json:"duration_ms"`
	RootService    string   `json:"root_service"`
	RootOperation  string   `json:"root_operation"`
	RootStatus     string   `json:"root_status,omitempty"`
	RootHTTPMethod string   `json:"root_http_method,omitempty"`
	RootHTTPStatus string   `json:"root_http_status,omitempty"`
	SpanCount      uint32   `json:"span_count"`
	HasError       bool     `json:"has_error"`
	ErrorCount     uint32   `json:"error_count"`
	ServiceSet     []string `json:"service_set,omitempty"`
	Truncated      bool     `json:"truncated,omitempty"`
}

type PageInfo struct {
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor,omitempty"`
	Limit      int    `json:"limit"`
}

type TraceCursor struct {
	StartMs uint64 `json:"s"`
	TraceID string `json:"t"`
}

func (c TraceCursor) IsZero() bool { return c.TraceID == "" }

func (c TraceCursor) Encode() string {
	if c.IsZero() {
		return ""
	}
	return cursor.Encode(c)
}

func DecodeCursor(raw string) (TraceCursor, bool) {
	return cursor.Decode[TraceCursor](raw)
}

type FacetBucket struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}

type Facets struct {
	Service    []FacetBucket `json:"service,omitempty"`
	Operation  []FacetBucket `json:"operation,omitempty"`
	HTTPMethod []FacetBucket `json:"http_method,omitempty"`
	HTTPStatus []FacetBucket `json:"http_status,omitempty"`
	Status     []FacetBucket `json:"status,omitempty"`
}

type TrendBucket struct {
	TimeBucket string `json:"time_bucket"`
	Total      uint64 `json:"total"`
	Errors     uint64 `json:"errors"`
}

type Suggestion struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}

type SuggestResponse struct {
	Suggestions []Suggestion `json:"suggestions"`
}

type CriticalPathSpan struct {
	SpanID        string  `json:"span_id"        ch:"span_id"`
	OperationName string  `json:"operation_name" ch:"operation_name"`
	ServiceName   string  `json:"service_name"   ch:"service"`
	DurationMs    float64 `json:"duration_ms"    ch:"duration_ms"`
}

type ErrorPathSpan struct {
	SpanID        string    `json:"span_id"        ch:"span_id"`
	ParentSpanID  string    `json:"parent_span_id" ch:"parent_span_id"`
	OperationName string    `json:"operation_name" ch:"operation_name"`
	ServiceName   string    `json:"service_name"   ch:"service"`
	Status        string    `json:"status"         ch:"status"`
	StatusMessage string    `json:"status_message" ch:"status_message"`
	StartTime     time.Time `json:"start_time"     ch:"start_time"`
	DurationMs    float64   `json:"duration_ms"    ch:"duration_ms"`
}

type ServiceMapNode struct {
	Service    string  `json:"service"`
	SpanCount  int     `json:"span_count"`
	ErrorCount int     `json:"error_count"`
	TotalMs    float64 `json:"total_ms"`
}

type ServiceMapEdge struct {
	From       string  `json:"from"`
	To         string  `json:"to"`
	CallCount  int     `json:"call_count"`
	ErrorCount int     `json:"error_count"`
	TotalMs    float64 `json:"total_ms"`
}

type ServiceMapResponse struct {
	Nodes []ServiceMapNode `json:"nodes"`
	Edges []ServiceMapEdge `json:"edges"`
}

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
