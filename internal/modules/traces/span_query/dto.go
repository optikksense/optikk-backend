package span_query //nolint:revive,stylecheck

import "github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"

// SpansQueryRequest is the wire payload for POST /api/v1/spans/query.
type SpansQueryRequest struct {
	StartTime int64                            `json:"startTime"`
	EndTime   int64                            `json:"endTime"`
	Filters   []querycompiler.StructuredFilter `json:"filters"`
	Limit     int                              `json:"limit"`
	Cursor    string                           `json:"cursor"`
}

// SpansQueryResponse is the wire response for POST /api/v1/spans/query.
type SpansQueryResponse struct {
	Results  []Span   `json:"results"`
	PageInfo PageInfo `json:"pageInfo"`
	Warnings []string `json:"warnings,omitempty"`
}

// spanRowDTO scans rows from observability.signoz_index_v3.
type spanRowDTO struct {
	SpanID             string `ch:"span_id"`
	TraceID            string `ch:"trace_id"`
	ParentSpanID       string `ch:"parent_span_id"`
	ServiceName        string `ch:"service_name"`
	Operation          string `ch:"name"`
	Kind               string `ch:"kind_string"`
	DurationNano       uint64 `ch:"duration_nano"`
	TimestampNs        int64  `ch:"timestamp_ns"`
	HasError           bool   `ch:"has_error"`
	Status             string `ch:"status_code_string"`
	HTTPMethod         string `ch:"http_method"`
	ResponseStatusCode string `ch:"response_status_code"`
	Environment        string `ch:"mat_deployment_environment"`
}
