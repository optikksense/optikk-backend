package logs

import (
	"fmt"
	"strconv"
	"strings"
)

// Log represents a single log entry.
type Log struct {
	ID                string             `json:"id"`
	Timestamp         uint64             `json:"timestamp"`
	ObservedTimestamp uint64             `json:"observedTimestamp"`
	SeverityText      string             `json:"severityText"`
	SeverityNumber    uint8              `json:"severityNumber"`
	Body              string             `json:"body"`
	TraceID           string             `json:"traceId"`
	SpanID            string             `json:"spanId"`
	TraceFlags        uint32             `json:"traceFlags"`
	ServiceName       string             `json:"serviceName"`
	Host              string             `json:"host"`
	Pod               string             `json:"pod"`
	Container         string             `json:"container"`
	Environment       string             `json:"environment"`
	AttributesString  map[string]string  `json:"attributesString,omitempty"`
	AttributesNumber  map[string]float64 `json:"attributesNumber,omitempty"`
	AttributesBool    map[string]bool    `json:"attributesBool,omitempty"`
	ScopeName         string             `json:"scopeName"`
	ScopeVersion      string             `json:"scopeVersion"`
	Resource          map[string]string  `json:"resource,omitempty"`
}

// LogFilters defines the search criteria for logs.
type LogFilters struct {
	TeamUUID     string   `json:"teamUuid"`
	StartMs      int64    `json:"startMs"`
	EndMs        int64    `json:"endMs"`
	Severities   []string `json:"severities"`
	Services     []string `json:"services"`
	Hosts        []string `json:"hosts"`
	Pods         []string `json:"pods"`
	Containers   []string `json:"containers"`
	Environments []string `json:"environments"`
	TraceID      string   `json:"traceId"`
	SpanID       string   `json:"spanId"`
	Search       string   `json:"search"`
	// SearchMode: "ngram" (default) uses ngramSearch for fast full-text;
	// "exact" uses positionCaseInsensitive for precise substring match.
	SearchMode string `json:"searchMode"`

	ExcludeSeverities []string `json:"excludeSeverities"`
	ExcludeServices   []string `json:"excludeServices"`
	ExcludeHosts      []string `json:"excludeHosts"`

	// AttributeFilters allows structured key=value filtering on attributes_string.
	// Each entry is a key-value pair: {Key: "user_id", Value: "123"}.
	AttributeFilters []LogAttributeFilter `json:"attributeFilters,omitempty"`
}

// LogAttributeFilter represents a single structured attribute key=value filter.
type LogAttributeFilter struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	// Op: "eq" (default), "neq", "contains", "regex"
	Op string `json:"op"`
}

// LogAggregateRequest represents the request body for log aggregation queries.
type LogAggregateRequest struct {
	GroupBy  string `json:"groupBy"`  // field name: severity_text, service, host, pod, etc.
	Step     string `json:"step"`     // time bucket: 1m, 5m, 1h, etc.
	TopN     int    `json:"topN"`     // return top-N groups (default 20)
	Metric   string `json:"metric"`   // "count" (default) or "error_rate"
}

// LogAggregateRow is one row in an aggregation response.
type LogAggregateRow struct {
	TimeBucket string `json:"timeBucket"`
	GroupValue string `json:"groupValue"`
	Count      int64  `json:"count"`
}

// LogCursor carries pagination state for deterministic ordering by (timestamp, id).
type LogCursor struct {
	Timestamp uint64 `json:"timestamp"` // nanoseconds
	ID        string `json:"id"`
	Offset    int    `json:"offset"`
}

func (c LogCursor) HasTimestamp() bool {
	return c.Timestamp > 0
}

func (c LogCursor) Encode() string {
	if c.Offset > 0 {
		return fmt.Sprintf("o:%d", c.Offset)
	}
	if c.ID == "" {
		return ""
	}
	if c.HasTimestamp() {
		return fmt.Sprintf("%d|%s", c.Timestamp, c.ID)
	}
	return c.ID
}

func ParseLogCursor(raw string) (LogCursor, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return LogCursor{}, false
	}

	if strings.HasPrefix(strings.ToLower(s), "o:") {
		offset, err := strconv.Atoi(strings.TrimSpace(s[2:]))
		if err != nil || offset <= 0 {
			return LogCursor{}, false
		}
		return LogCursor{Offset: offset}, true
	}

	parts := strings.SplitN(s, "|", 2)
	if len(parts) == 2 {
		tsRaw := strings.TrimSpace(parts[0])
		idRaw := strings.TrimSpace(parts[1])

		ts, err := strconv.ParseUint(tsRaw, 10, 64)
		if err != nil || ts == 0 {
			return LogCursor{}, false
		}
		if idRaw == "" {
			return LogCursor{}, false
		}

		return LogCursor{
			Timestamp: ts,
			ID:        idRaw,
		}, true
	}

	return LogCursor{}, false
}

// LogHistogramBucket represents a time-bucketed count of logs by severity.
type LogHistogramBucket struct {
	TimeBucket string `json:"timeBucket"`
	Severity   string `json:"severity"`
	Count      int64  `json:"count"`
}

// LogHistogramData represents the complete histogram response.
type LogHistogramData struct {
	Buckets []LogHistogramBucket `json:"buckets"`
	Step    string               `json:"step"`
}

// LogVolumeBucket represents per-bucket totals with severity breakdown.
type LogVolumeBucket struct {
	TimeBucket string `json:"timeBucket"`
	Total      int64  `json:"total"`
	Errors     int64  `json:"errors"`
	Warnings   int64  `json:"warnings"`
	Infos      int64  `json:"infos"`
	Debugs     int64  `json:"debugs"`
	Fatals     int64  `json:"fatals"`
}

// LogVolumeData represents the complete log volume response.
type LogVolumeData struct {
	Buckets []LogVolumeBucket `json:"buckets"`
	Step    string            `json:"step"`
}

// LogStats represents aggregate statistics and facets for logs.
type LogStats struct {
	Total  int64              `json:"total"`
	Fields map[string][]Facet `json:"fields"`
}

// Facet represents a single facet value and its count.
type Facet struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

// LogSearchResponse represents the result of a log search with pagination and facets.
type LogSearchResponse struct {
	Logs       []Log  `json:"logs"`
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor"`
	Limit      int    `json:"limit"`
	Total      int64  `json:"total"`
}

// LogSurroundingResponse represents a log entry with its surrounding context.
type LogSurroundingResponse struct {
	Anchor Log   `json:"anchor"`
	Before []Log `json:"before"`
	After  []Log `json:"after"`
}

// LogDetailResponse represents a single log entry and its service context.
type LogDetailResponse struct {
	Log         Log   `json:"log"`
	ContextLogs []Log `json:"contextLogs"`
}

// TraceLogsResponse represents trace logs, separating direct matches from speculative matches.
type TraceLogsResponse struct {
	Logs          []Log `json:"logs"`
	IsSpeculative bool  `json:"isSpeculative"`
}

// LogFacetsResponse represents facet breakdowns for facet-only queries.
type LogFacetsResponse struct {
	Total  int64              `json:"total"`
	Facets map[string][]Facet `json:"facets"`
}
