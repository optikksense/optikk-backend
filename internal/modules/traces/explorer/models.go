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
