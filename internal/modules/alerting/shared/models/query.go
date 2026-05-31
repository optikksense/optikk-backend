package models

// MonitorQuery is a discriminated union over the supported monitor types.
// Exactly one of Metric / APM / Log is populated, matching the row's `type`.
// The evaluator dispatches on the monitor type to the corresponding shared/query
// implementation; CRUD validates each shape via Validate().
type MonitorQuery struct {
	Metric *MetricQuery `json:"metric,omitempty"`
	APM    *APMQuery    `json:"apm,omitempty"`
	Log    *LogQuery    `json:"log,omitempty"`
}

// MetricQuery: aggregate a metric over a window with optional resource tags.
type MetricQuery struct {
	Metric      string `json:"metric"`
	Aggregation string `json:"aggregation"` // avg | sum | min | max | p50 | p95 | p99
	WindowSec   int    `json:"window_sec"`  // 60, 300, 900, 3600
}

// APMQuery: track an apm signal on a service+resource.
type APMQuery struct {
	Service   string `json:"service"`
	Resource  string `json:"resource,omitempty"`
	Track     string `json:"track"` // errors | hits | latency | apdex
	WindowSec int    `json:"window_sec"`
}

// LogQuery: count log events matching a query, grouped optionally.
type LogQuery struct {
	Query     string `json:"query"`
	GroupBy   string `json:"group_by,omitempty"` // service | host | status | none
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
