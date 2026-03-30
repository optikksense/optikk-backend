package shared

import (
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
	SearchMode   string   `json:"searchMode"`

	ExcludeSeverities []string `json:"excludeSeverities"`
	ExcludeServices   []string `json:"excludeServices"`
	ExcludeHosts      []string `json:"excludeHosts"`

	AttributeFilters []LogAttributeFilter `json:"attributeFilters,omitempty"`
}

type LogAttributeFilter struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Op    string `json:"op"`
}

type LogCursor struct {
	Offset int
}

func (c LogCursor) Encode() string {
	if c.Offset <= 0 {
		return ""
	}
	return strconv.Itoa(c.Offset)
}

func ParseLogCursor(raw string) (LogCursor, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return LogCursor{}, false
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return LogCursor{}, false
	}
	return LogCursor{Offset: n}, true
}
