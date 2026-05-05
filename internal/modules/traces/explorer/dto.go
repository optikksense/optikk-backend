package explorer

import (
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

// QueryRequest is the wire payload for POST /api/v1/traces/query. Filters
// are embedded directly (no separate compile pass).
type QueryRequest struct {
	StartTime int64    `json:"startTime"`
	EndTime   int64    `json:"endTime"`
	Include   []string `json:"include"`
	Limit     int      `json:"limit"`
	Cursor    string   `json:"cursor"`

	filter.Filters
}

// QueryResponse is the wire response for POST /api/v1/traces/query.
type QueryResponse struct {
	Results  []Trace       `json:"results"`
	Facets   *Facets       `json:"facets,omitempty"`
	Trend    []TrendBucket `json:"trend,omitempty"`
	PageInfo PageInfo      `json:"pageInfo"`
}

// traceIndexRowDTO scans rows from observability.spans (root spans).
// All temporal fields scan native CH DateTime/DateTime64 into time.Time;
// conversion to wire-model Unix-millis happens in service.go.
type traceIndexRowDTO struct {
	TraceID        string    `ch:"trace_id"`
	StartTime      time.Time `ch:"start_time"`
	EndTime        time.Time `ch:"end_time"`
	DurationNs     uint64    `ch:"duration_ns"`
	RootService    string    `ch:"root_service"`
	RootOperation  string    `ch:"root_operation"`
	RootStatus     string    `ch:"root_status"`
	RootHTTPMethod string    `ch:"root_http_method"`
	RootHTTPStatus uint16    `ch:"root_http_status"`
	SpanCount      uint32    `ch:"span_count"`
	HasError       bool      `ch:"has_error"`
	ErrorCount     uint32    `ch:"error_count"`
	ServiceSet     []string  `ch:"service_set"`
	Truncated      bool      `ch:"truncated"`
	LastSeen       time.Time `ch:"last_seen"`
}

type trendRowDTO struct {
	TimeBucket string `ch:"time_bucket"`
	Status     string `ch:"status"`
	Count      uint64 `ch:"count"`
}

type facetRowDTO struct {
	Dim   string `ch:"dim"`
	Value string `ch:"value"`
	Count uint64 `ch:"count"`
}
