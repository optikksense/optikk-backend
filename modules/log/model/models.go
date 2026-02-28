package model

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Log represents a single log entry.
type Log struct {
	ID          string    `json:"id"`
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

// LogCursor carries pagination state for deterministic ordering by timestamp,id.
type LogCursor struct {
	Timestamp time.Time `json:"timestamp"`
	ID        uint64    `json:"id"`
	Offset    int       `json:"offset"`
}

func (c LogCursor) HasTimestamp() bool {
	return !c.Timestamp.IsZero()
}

func (c LogCursor) Encode() string {
	if c.Offset > 0 {
		return fmt.Sprintf("o:%d", c.Offset)
	}
	if c.ID == 0 {
		return ""
	}
	if c.HasTimestamp() {
		return fmt.Sprintf("%s|%d", c.Timestamp.UTC().Format(time.RFC3339Nano), c.ID)
	}
	// Backward-compatible legacy cursor (id only)
	return strconv.FormatUint(c.ID, 10)
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

		id, err := strconv.ParseUint(idRaw, 10, 64)
		if err != nil || id == 0 {
			// Backward compatibility for signed cursor IDs.
			signedID, signedErr := strconv.ParseInt(idRaw, 10, 64)
			if signedErr != nil || signedID <= 0 {
				return LogCursor{}, false
			}
			id = uint64(signedID)
		}

		ts, err := time.Parse(time.RFC3339Nano, tsRaw)
		if err != nil {
			// Fallback for driver-provided timestamp strings without timezone.
			ts, err = time.Parse("2006-01-02 15:04:05.999999999", tsRaw)
			if err != nil {
				return LogCursor{}, false
			}
		}

		return LogCursor{
			Timestamp: ts.UTC(),
			ID:        id,
		}, true
	}

	// Backward compatibility: old clients send id-only cursor.
	// Numeric cursor defaults to offset mode for robust pagination when ids are non-unique.
	offset, offsetErr := strconv.Atoi(s)
	if offsetErr == nil && offset > 0 {
		return LogCursor{Offset: offset}, true
	}

	id, err := strconv.ParseUint(s, 10, 64)
	if err == nil && id > 0 {
		return LogCursor{ID: id}, true
	}
	signedID, signedErr := strconv.ParseInt(s, 10, 64)
	if signedErr != nil || signedID <= 0 {
		return LogCursor{}, false
	}
	return LogCursor{ID: uint64(signedID)}, true
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

// LogQueryResult is the repository-level result for paginated log queries.
type LogQueryResult struct {
	Logs          []Log
	Total         int64
	LevelFacets   []Facet
	ServiceFacets []Facet
	HostFacets    []Facet
}

// LogStatsResult is the repository-level result for log statistics.
type LogStatsResult struct {
	Total         int64
	LevelFacets   []Facet
	ServiceFacets []Facet
	HostFacets    []Facet
	PodFacets     []Facet
	LoggerFacets  []Facet
}

// LogSearchResponse represents the result of a log search with pagination and facets.
type LogSearchResponse struct {
	Logs       []Log              `json:"logs"`
	HasMore    bool               `json:"hasMore"`
	NextCursor string             `json:"nextCursor"`
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

// TraceLogsResponse represents trace logs, separating direct matches from speculative matches.
type TraceLogsResponse struct {
	Logs        []Log `json:"logs"`
	IsSpeculative bool  `json:"isSpeculative"`
}
