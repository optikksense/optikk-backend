package livetail

import (
	"strconv"
	"strings"
	"time"
)

// LiveSpan is a simplified span emitted to clients (e.g. live tail WebSocket).
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

// MatchesSpan applies the same filter dimensions as repository Poll (best-effort for Redis-sourced spans).
func (f LiveTailFilters) MatchesSpan(s LiveSpan) bool {
	if len(f.Services) > 0 {
		ok := false
		for _, svc := range f.Services {
			if svc == s.ServiceName {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if f.Status == "ERROR" {
		if s.HasError {
			return true
		}
		code, err := strconv.Atoi(strings.TrimSpace(s.HTTPStatus))
		if err == nil && code >= 400 {
			return true
		}
		return false
	}
	if f.Status != "" && s.Status != f.Status {
		return false
	}
	if f.SpanKind != "" && s.SpanKind != f.SpanKind {
		return false
	}
	if f.Operation != "" && !strings.Contains(strings.ToLower(s.OperationName), strings.ToLower(f.Operation)) {
		return false
	}
	if f.HTTPMethod != "" && strings.ToUpper(strings.TrimSpace(f.HTTPMethod)) != strings.ToUpper(strings.TrimSpace(s.HTTPMethod)) {
		return false
	}
	if f.SearchText != "" {
		q := strings.ToLower(f.SearchText)
		return strings.Contains(strings.ToLower(s.TraceID), q) ||
			strings.Contains(strings.ToLower(s.ServiceName), q) ||
			strings.Contains(strings.ToLower(s.OperationName), q) ||
			strings.Contains(strings.ToLower(s.Status), q)
	}
	return true
}

// redisSpanTailMsg is JSON from OTLP livetail bridge (matches otlp/spans spanLiveTailJSON + LiveSpan).
type redisSpanTailMsg struct {
	LiveSpan
	EmitMs int64 `json:"emit_ms"`
}
