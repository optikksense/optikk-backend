package models

// MonitorQuery is a discriminated union over supported monitor types.
// Dispatches to the corresponding shared/query implementation.
type MonitorQuery struct {
	Metric *MetricQuery `json:"metric,omitempty"`
	APM    *APMQuery    `json:"apm,omitempty"`
	Log    *LogQuery    `json:"log,omitempty"`
}

// MetricQuery: aggregate a metric over a window with optional resource tags.
type MetricQuery struct {
	Metric      string `json:"metric"`
	// Aggregation can be avg, sum, min, max, p50, p95, or p99.
	Aggregation string `json:"aggregation"`
	// WindowSec is the window size in seconds (e.g. 60, 300, 900, 3600).
	WindowSec   int    `json:"window_sec"`
}

// APMQuery: track an apm signal on a service+resource.
type APMQuery struct {
	Service   string `json:"service"`
	Resource  string `json:"resource,omitempty"`
	// Track can be errors, hits, latency, or apdex.
	Track     string `json:"track"`
	WindowSec int    `json:"window_sec"`
}

// LogQuery: count log events matching a query, grouped optionally.
type LogQuery struct {
	Query     string `json:"query"`
	// GroupBy can be service, host, status, or none.
	GroupBy   string `json:"group_by,omitempty"`
	WindowSec int    `json:"window_sec"`
}

// NotifyTargets is the channel-ID list a monitor dispatches to.
type NotifyTargets struct {
	ChannelIDs []int64 `json:"channel_ids"`
}

// SupportedMonitorTypes lists the three v1 types in canonical order.
var SupportedMonitorTypes = []string{"metric", "apm", "log"}

// SupportedPriorities lists the four priorities in display order.
var SupportedPriorities = []string{"P1", "P2", "P3", "P4"}

// IsValidType reports whether t is one of the supported monitor types.
func IsValidType(t string) bool {
	for _, v := range SupportedMonitorTypes {
		if v == t {
			return true
		}
	}
	return false
}

// IsValidPriority reports whether p is one of the supported priorities.
func IsValidPriority(p string) bool {
	for _, v := range SupportedPriorities {
		if v == p {
			return true
		}
	}
	return false
}
