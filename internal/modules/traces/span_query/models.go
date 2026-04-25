// Package span_query exposes POST /api/v1/spans/query — the span-level explorer
// view (Datadog-style "Spans" tab), split out of the legacy explorer module.
package span_query //nolint:revive,stylecheck

import "github.com/Optikk-Org/optikk-backend/internal/infra/cursor"

// Span is the wire row for /spans/query — individual spans, not trace summaries.
type Span struct {
	SpanID             string  `json:"span_id"`
	TraceID            string  `json:"trace_id"`
	ParentSpanID       string  `json:"parent_span_id,omitempty"`
	ServiceName        string  `json:"service_name"`
	Operation          string  `json:"operation"`
	Kind               string  `json:"kind,omitempty"`
	DurationMs         float64 `json:"duration_ms"`
	TimestampNs        int64   `json:"timestamp_ns"`
	HasError           bool    `json:"has_error"`
	Status             string  `json:"status,omitempty"`
	HTTPMethod         string  `json:"http_method,omitempty"`
	ResponseStatusCode string  `json:"response_status_code,omitempty"`
	Environment        string  `json:"environment,omitempty"`
}

// SpanCursor is the keyset pagination cursor for spans (newest-first).
type SpanCursor struct {
	TimestampNs int64  `json:"ts"`
	SpanID      string `json:"s"`
}

func (c SpanCursor) IsZero() bool { return c.SpanID == "" }

func (c SpanCursor) Encode() string {
	if c.IsZero() {
		return ""
	}
	return cursor.Encode(c)
}

func DecodeSpanCursor(raw string) (SpanCursor, bool) {
	return cursor.Decode[SpanCursor](raw)
}

// PageInfo matches the explorer pagination envelope.
type PageInfo struct {
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor,omitempty"`
	Limit      int    `json:"limit"`
}
