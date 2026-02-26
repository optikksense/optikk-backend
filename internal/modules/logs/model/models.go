package model

import "time"

// Log represents a single log entry.
type Log struct {
	ID          int64     `json:"id"`
	Timestamp   time.Time `json:"timestamp"`
	Level       string    `json:"level"`
	ServiceName string    `json:"serviceName"`
	Logger      string    `json:"logger"`
	Message     string    `json:"message"`
	TraceID     string    `json:"traceId"`
	SpanID      string    `json:"spanId"`
	Host        string    `json:"host"`
	Pod         string    `json:"pod"`
	Container   string    `json:"container"`
	Thread      string    `json:"thread"`
	Exception   string    `json:"exception"`
	Attributes  string    `json:"attributes"`
}

// LogFilters defines the search criteria for logs.
type LogFilters struct {
	TeamUUID   string   `json:"teamUuid"`
	StartMs    int64    `json:"startMs"`
	EndMs      int64    `json:"endMs"`
	Levels     []string `json:"levels"`
	Services   []string `json:"services"`
	Hosts      []string `json:"hosts"`
	Pods       []string `json:"pods"`
	Containers []string `json:"containers"`
	Loggers    []string `json:"loggers"`
	TraceID    string   `json:"traceId"`
	SpanID     string   `json:"spanId"`
	Search     string   `json:"search"`

	ExcludeLevels   []string `json:"excludeLevels"`
	ExcludeServices []string `json:"excludeServices"`
	ExcludeHosts    []string `json:"excludeHosts"`
}

// LogHistogramBucket represents a time-bucketed count of logs by level.
type LogHistogramBucket struct {
	TimeBucket string `json:"timeBucket"`
	Level      string `json:"level"`
	Count      int64  `json:"count"`
}

// LogHistogramData represents the complete histogram response.
type LogHistogramData struct {
	Buckets []LogHistogramBucket `json:"buckets"`
	Step    string               `json:"step"`
}

// LogVolumeBucket represents per-bucket totals with level breakdown.
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
	Logs       []Log              `json:"logs"`
	HasMore    bool               `json:"hasMore"`
	NextCursor int64              `json:"nextCursor"`
	Limit      int                `json:"limit"`
	Total      int64              `json:"total"`
	Facets     map[string][]Facet `json:"facets"`
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
