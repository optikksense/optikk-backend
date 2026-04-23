package explorer

import "github.com/Optikk-Org/optikk-backend/internal/infra/cursor"

type Trace struct {
	TraceID        string   `json:"trace_id"`
	StartMs        uint64   `json:"start_ms"`
	EndMs          uint64   `json:"end_ms"`
	DurationMs     float64  `json:"duration_ms"`
	RootService    string   `json:"root_service"`
	RootOperation  string   `json:"root_operation"`
	RootStatus     string   `json:"root_status,omitempty"`
	RootHTTPMethod string   `json:"root_http_method,omitempty"`
	RootHTTPStatus uint16   `json:"root_http_status,omitempty"`
	SpanCount      uint32   `json:"span_count"`
	HasError       bool     `json:"has_error"`
	ErrorCount     uint32   `json:"error_count"`
	ServiceSet     []string `json:"service_set,omitempty"`
	Truncated      bool     `json:"truncated,omitempty"`
}

type Summary struct {
	TotalTraces   uint64 `json:"total_traces"`
	TotalErrors   uint64 `json:"total_errors"`
	TotalDuration uint64 `json:"total_duration_ns"` // sum(duration_ns) is UInt64 in ClickHouse
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

type PageInfo struct {
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor,omitempty"`
	Limit      int    `json:"limit"`
}

type AnalyticsRow map[string]any

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
