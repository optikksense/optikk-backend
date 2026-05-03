// Package models holds wire + domain types shared by every logs submodule (explorer, logdetail, log_facets, log_trends).
package models

import (
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/cursor"
)

// Log is the JSON model for a single log row returned by list + detail.
type Log struct {
	ID                string             `json:"id"`
	Timestamp         uint64             `json:"timestamp,string"`
	ObservedTimestamp uint64             `json:"observed_timestamp,string"`
	SeverityText      string             `json:"severity_text"`
	SeverityNumber    uint8              `json:"severity_number"`
	SeverityBucket    uint8              `json:"severity_bucket"`
	Body              string             `json:"body"`
	TraceID           string             `json:"trace_id"`
	SpanID            string             `json:"span_id"`
	TraceFlags        uint32             `json:"trace_flags"`
	ServiceName       string             `json:"service_name"`
	Host              string             `json:"host"`
	Pod               string             `json:"pod"`
	Container         string             `json:"container"`
	Environment       string             `json:"environment"`
	AttributesString  map[string]string  `json:"attributes_string,omitempty"`
	AttributesNumber  map[string]float64 `json:"attributes_number,omitempty"`
	AttributesBool    map[string]bool    `json:"attributes_bool,omitempty"`
	ScopeName         string             `json:"scope_name"`
	ScopeVersion      string             `json:"scope_version"`
}

// LogRow is the ClickHouse scan target for raw log reads.
type LogRow struct {
	LogID             string             `ch:"log_id"`
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

// Cursor keyset-paginates raw logs ordered by (timestamp, observed_timestamp,
// trace_id) DESC. Zero cursor means "first page".
type Cursor struct {
	Timestamp         time.Time `json:"ts"`
	ObservedTimestamp uint64    `json:"ots"`
	TraceID           string    `json:"tid"`
}

func (c Cursor) IsZero() bool {
	return c.Timestamp.IsZero() && c.ObservedTimestamp == 0 && c.TraceID == ""
}

func (c Cursor) Encode() string {
	if c.IsZero() {
		return ""
	}
	return cursor.Encode(c)
}

func DecodeCursor(raw string) (Cursor, bool) {
	return cursor.Decode[Cursor](raw)
}

// FacetValue is one bucket in a facet group.
type FacetValue struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}

// SeverityLabels is the closed set of severity-bucket labels (mirrors the
// on-disk severity_bucket → text mapping in the logs ingest mapper). Returned
// directly by /logs/facets — severity is a fixed enum, no DB query needed.
var SeverityLabels = []string{"UNSET", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

// Facets groups per-dim top-N counts. Severity is a static label list (no
// counts) since the values are a closed enum; resource dims are top-N from
// observability.logs_resource.
type Facets struct {
	Severity    []string     `json:"severity_bucket"`
	Service     []FacetValue `json:"service"`
	Host        []FacetValue `json:"host,omitempty"`
	Pod         []FacetValue `json:"pod,omitempty"`
	Environment []FacetValue `json:"environment,omitempty"`
}

// Summary captures compact KPIs for the list view header.
type Summary struct {
	Total  uint64 `json:"total"`
	Errors uint64 `json:"errors"`
	Warns  uint64 `json:"warns"`
}

// TrendBucket is a single severity-bucketed histogram bar.
type TrendBucket struct {
	TimeBucket string `json:"time_bucket"`
	Severity   uint8  `json:"severity_bucket"`
	Count      uint64 `json:"count"`
}

// PageInfo carries cursor state.
type PageInfo struct {
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor,omitempty"`
	Limit      int    `json:"limit"`
}
