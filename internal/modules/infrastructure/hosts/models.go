package hosts

import (
	"strings"
	"time"
)

// HostStatus is the health classification for a host running a service.
type HostStatus string

const (
	HostHealthy HostStatus = "healthy"
	HostWarn    HostStatus = "warn"
	HostError   HostStatus = "error"
)

// Host is the unified host resource representation. RED traffic fields
// are populated only when scoped to a service.
type Host struct {
	Host string `json:"host"`
	// Subsystem is "kafka", "database", or "other".
	Subsystem string  `json:"subsystem"`
	CPU       float64 `json:"cpu"`
	Mem       float64 `json:"mem"`
	Disk      float64 `json:"disk"`
	// Saturation is the max of CPU, memory, and disk.
	Saturation float64 `json:"saturation"`
	// Tone is "ok", "warn", or "err".
	Tone string `json:"tone"`

	// Service-scoped enrichment — present only when filtered by service.
	Zone         string     `json:"zone,omitempty"`
	RPS          *float64   `json:"rps,omitempty"`
	ErrorRate    *float64   `json:"error_rate,omitempty"`
	P99Ms        *float64   `json:"p99_ms,omitempty"`
	Status       HostStatus `json:"status,omitempty"`
	LastSeen     string     `json:"last_seen,omitempty"`
	RequestCount int64      `json:"request_count,omitempty"`
	ErrorCount   int64      `json:"error_count,omitempty"`
}

// hostMetricRow is one (host, metric) window-average.
type hostMetricRow struct {
	Host       string  `ch:"host"`
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}

// hostSpansRow is the per-host RED aggregate read from spans_1m.
type hostSpansRow struct {
	Host         string    `ch:"host"`
	Zone         string    `ch:"zone"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
	P99Ms        float32   `ch:"p99_ms"`
	LastSeen     time.Time `ch:"last_seen"`
}

// Subsystem buckets.
const (
	SubsystemKafka    = "kafka"
	SubsystemDatabase = "database"
	SubsystemOther    = "other"
)

// subsystemForHost maps a host to a subsystem by prefix (e.g. kafka, db).
func subsystemForHost(host string) string {
	h := strings.ToLower(host)
	switch {
	case strings.HasPrefix(h, "kafka") || strings.Contains(h, "broker"):
		return SubsystemKafka
	case strings.HasPrefix(h, "pg") || strings.HasPrefix(h, "postgres") || strings.HasPrefix(h, "mysql") || strings.HasPrefix(h, "db"):
		return SubsystemDatabase
	default:
		return SubsystemOther
	}
}

// toneForSaturation matches the design's tiers: ≥90 error, ≥70 warn, else ok.
func toneForSaturation(pct float64) string {
	switch {
	case pct >= 90:
		return "err"
	case pct >= 70:
		return "warn"
	default:
		return "ok"
	}
}
