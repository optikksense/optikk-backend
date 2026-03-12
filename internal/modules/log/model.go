package logs

import (
	"fmt"
	"strconv"
	"strings"
)

type Log struct {
	ID                string             `json:"id"`
	Timestamp         uint64             `json:"timestamp"`
	ObservedTimestamp uint64             `json:"observed_timestamp"`
	SeverityText      string             `json:"severity_text"`
	SeverityNumber    uint8              `json:"severity_number"`
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

type LogFilters struct {
	TeamID       int64    `json:"teamId"`
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

type LogAttributeFilter struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	// Op: "eq" (default), "neq", "contains", "regex"
	Op string `json:"op"`
}

type LogAggregateRequest struct {
	GroupBy string `json:"group_by" form:"group_by"` // field name: severity_text, service, host, pod, etc.
	Step    string `json:"step"     form:"step"`      // time bucket: 1m, 5m, 1h, etc.
	TopN    int    `json:"top_n"    form:"top_n"`     // return top-N groups (default 20)
	Metric  string `json:"metric"   form:"metric"`    // "count" (default) or "error_rate"
}

type LogAggregateRow struct {
	TimeBucket string  `json:"time_bucket"`
	GroupValue string  `json:"group_value"`
	Count      int64   `json:"count"`
	ErrorRate  float64 `json:"error_rate,omitempty"`
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

type LogHistogramBucket struct {
	TimeBucket string `json:"time_bucket"`
	Severity   string `json:"severity"`
	Count      int64  `json:"count"`
}

type LogHistogramData struct {
	Buckets []LogHistogramBucket `json:"buckets"`
	Step    string               `json:"step"`
}

type LogVolumeBucket struct {
	TimeBucket string `json:"time_bucket"`
	Total      int64  `json:"total"`
	Errors     int64  `json:"errors"`
	Warnings   int64  `json:"warnings"`
	Infos      int64  `json:"infos"`
	Debugs     int64  `json:"debugs"`
	Fatals     int64  `json:"fatals"`
}

type LogVolumeData struct {
	Buckets []LogVolumeBucket `json:"buckets"`
	Step    string            `json:"step"`
}

type LogStats struct {
	Total  int64              `json:"total"`
	Fields map[string][]Facet `json:"fields"`
}

type Facet struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type LogSearchResponse struct {
	Logs       []Log  `json:"logs"`
	HasMore    bool   `json:"has_more"`
	NextCursor string `json:"next_cursor,omitempty"`
	Limit      int    `json:"limit"`
	Total      int64  `json:"total"`
}

type LogSurroundingResponse struct {
	Anchor Log   `json:"anchor"`
	Before []Log `json:"before"`
	After  []Log `json:"after"`
}

type LogDetailResponse struct {
	Log         Log   `json:"log"`
	ContextLogs []Log `json:"contextLogs"`
}

type TraceLogsResponse struct {
	Logs          []Log `json:"logs"`
	IsSpeculative bool  `json:"isSpeculative"`
}

