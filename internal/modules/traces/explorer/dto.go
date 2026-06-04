package explorer

import (
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

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
