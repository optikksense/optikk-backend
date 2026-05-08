package span_query //nolint:revive,stylecheck

import (
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

// SpansQueryRequest is the wire payload for POST /api/v1/spans/query.
// Filters are embedded directly (no separate compile pass).
type SpansQueryRequest struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Limit     int    `json:"limit"`
	Cursor    string `json:"cursor"`

	filter.Filters
}

// SpansQueryResponse is the wire response for POST /api/v1/spans/query.
type SpansQueryResponse struct {
	Results  []Span   `json:"results"`
	PageInfo PageInfo `json:"pageInfo"`
}

// spanRowDTO scans rows from observability.spans. Timestamp is scanned as a
// native CH DateTime64; the service layer converts to nanoseconds for the
// wire payload + cursor encoding.
type spanRowDTO struct {
	SpanID             string    `ch:"span_id"`
	TraceID            string    `ch:"trace_id"`
	ParentSpanID       string    `ch:"parent_span_id"`
	ServiceName        string    `ch:"service"`
	Operation          string    `ch:"name"`
	Kind               string    `ch:"kind_string"`
	DurationNano       uint64    `ch:"duration_nano"`
	Timestamp          time.Time `ch:"timestamp"`
	HasError           bool      `ch:"has_error"`
	Status             string    `ch:"status_code_string"`
	HTTPMethod         string    `ch:"http_method"`
	ResponseStatusCode string    `ch:"response_status_code"`
	Environment        string    `ch:"environment"`
}
