package explorer

import (
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
)

// QueryRequest is the wire payload for POST /api/v1/logs/query.
type QueryRequest struct {
	StartTime int64                            `json:"startTime"`
	EndTime   int64                            `json:"endTime"`
	Filters   []querycompiler.StructuredFilter `json:"filters"`
	Include   []string                         `json:"include"`
	Limit     int                              `json:"limit"`
	Cursor    string                           `json:"cursor"`
}

// QueryResponse is the wire response for POST /api/v1/logs/query.
type QueryResponse struct {
	Results  []Log         `json:"results"`
	Summary  *Summary      `json:"summary,omitempty"`
	Facets   *Facets       `json:"facets,omitempty"`
	Trend    []TrendBucket `json:"trend,omitempty"`
	PageInfo PageInfo      `json:"pageInfo"`
	Warnings []string      `json:"warnings,omitempty"`
}

// Aggregation describes a single aggregation fn applied to a field.
type Aggregation struct {
	Fn    string `json:"fn"`    // count | count_distinct | sum | avg | min | max | p50 | p90 | p95 | p99
	Field string `json:"field"` // required except for "count"
	Alias string `json:"alias"` // output column name
}

// AnalyticsRequest is the wire payload for POST /api/v1/logs/analytics.
type AnalyticsRequest struct {
	StartTime    int64                            `json:"startTime"`
	EndTime      int64                            `json:"endTime"`
	Filters      []querycompiler.StructuredFilter `json:"filters"`
	GroupBy      []string                         `json:"groupBy"`
	Aggregations []Aggregation                    `json:"aggregations"`
	Step         string                           `json:"step"`
	VizMode      string                           `json:"vizMode"` // timeseries | topN | table | pie
	Limit        int                              `json:"limit"`
	OrderBy      string                           `json:"orderBy"`
}

// AnalyticsResponse carries the rendered grid + viz metadata.
type AnalyticsResponse struct {
	VizMode  string         `json:"vizMode"`
	Step     string         `json:"step,omitempty"`
	Rows     []AnalyticsRow `json:"rows"`
	Warnings []string       `json:"warnings,omitempty"`
}

// logRowDTO is the ClickHouse scan target for raw log reads.
type logRowDTO struct {
	Timestamp         time.Time          `ch:"timestamp"`
	ObservedTimestamp uint64             `ch:"observed_timestamp"`
	SeverityText      string             `ch:"severity_text"`
	SeverityNumber    uint8              `ch:"severity_number"`
	SeverityBucket    uint8              `ch:"severity_bucket"`
	Body              string             `ch:"body"`
	TraceID           string             `ch:"trace_id"`
	SpanID            string             `ch:"span_id"`
	TraceFlags        uint32             `ch:"trace_flags"`
	ServiceName       string             `ch:"service"`
	Host              string             `ch:"host"`
	Pod               string             `ch:"pod"`
	Container         string             `ch:"container"`
	Environment       string             `ch:"environment"`
	AttributesString  map[string]string  `ch:"attributes_string"`
	AttributesNumber  map[string]float64 `ch:"attributes_number"`
	AttributesBool    map[string]bool    `ch:"attributes_bool"`
	ScopeName         string             `ch:"scope_name"`
	ScopeVersion      string             `ch:"scope_version"`
}

// trendRowDTO scans the severity-bucketed histogram from the rollup.
type trendRowDTO struct {
	TimeBucket string `ch:"time_bucket"`
	Severity   uint8  `ch:"severity_bucket"`
	Count      uint64 `ch:"count"`
}

// facetRowDTO is a multi-dim facet count row. Dim identifies which column
// the value came from so the service can pivot to typed facet groups.
type facetRowDTO struct {
	Dim   string `ch:"dim"`
	Value string `ch:"value"`
	Count uint64 `ch:"count"`
}
