package explorer

import (
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

// QueryRequest is the wire payload for POST /api/v1/traces/query.
type QueryRequest struct {
	StartTime int64                            `json:"startTime"`
	EndTime   int64                            `json:"endTime"`
	Filters   []querycompiler.StructuredFilter `json:"filters"`
	Include   []string                         `json:"include"`
	Limit     int                              `json:"limit"`
	Cursor    string                           `json:"cursor"`
}

// QueryResponse is the wire response for POST /api/v1/traces/query.
type QueryResponse struct {
	Results  []Trace       `json:"results"`
	Summary  *Summary      `json:"summary,omitempty"`
	Facets   *Facets       `json:"facets,omitempty"`
	Trend    []TrendBucket `json:"trend,omitempty"`
	PageInfo PageInfo      `json:"pageInfo"`
	Warnings []string      `json:"warnings,omitempty"`
}

type Aggregation struct {
	Fn    string `json:"fn"`
	Field string `json:"field"`
	Alias string `json:"alias"`
}

// AnalyticsRequest is the wire payload for POST /api/v1/traces/analytics.
type AnalyticsRequest struct {
	StartTime    int64                            `json:"startTime"`
	EndTime      int64                            `json:"endTime"`
	Filters      []querycompiler.StructuredFilter `json:"filters"`
	GroupBy      []string                         `json:"groupBy"`
	Aggregations []Aggregation                    `json:"aggregations"`
	Step         string                           `json:"step"`
	VizMode      string                           `json:"vizMode"`
	Limit        int                              `json:"limit"`
	OrderBy      string                           `json:"orderBy"`
}

type AnalyticsResponse struct {
	VizMode  string         `json:"vizMode"`
	Step     string         `json:"step,omitempty"`
	Rows     []AnalyticsRow `json:"rows"`
	Warnings []string       `json:"warnings,omitempty"`
}

// traceIndexRowDTO scans rows from observability.traces_index.
type traceIndexRowDTO struct {
	TraceID        string    `ch:"trace_id"`
	StartMs        int64     `ch:"start_ms"`
	EndMs          int64     `ch:"end_ms"`
	DurationNs     int64     `ch:"duration_ns"`
	RootService    string    `ch:"root_service"`
	RootOperation  string    `ch:"root_operation"`
	RootStatus     string    `ch:"root_status"`
	RootHTTPMethod string    `ch:"root_http_method"`
	RootHTTPStatus string    `ch:"root_http_status"`
	SpanCount      uint32    `ch:"span_count"`
	HasError       bool      `ch:"has_error"`
	ErrorCount     uint32    `ch:"error_count"`
	ServiceSet     []string  `ch:"service_set"`
	Truncated      bool      `ch:"truncated"`
	LastSeenMs     time.Time `ch:"last_seen_ms"`
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
