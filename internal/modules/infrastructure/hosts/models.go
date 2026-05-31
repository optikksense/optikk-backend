package hosts

import (
	"strings"
	"time"
)

// HostStatus is the RED-derived health classification for a host running a
// service. Mirrors the dot-color scheme used by the design (healthy / warn / error).
type HostStatus string

const (
	HostHealthy HostStatus = "healthy"
	HostWarn    HostStatus = "warn"
	HostError   HostStatus = "error"
)

// Host is the unified per-host row served by GET /infrastructure/hosts. The
// resource-saturation fields are always present; the RED traffic fields are
// populated only when the request is scoped to a service (?service=).
type Host struct {
	Host       string  `json:"host"`
	Subsystem  string  `json:"subsystem"` // "kafka" | "database" | "other"
	CPU        float64 `json:"cpu"`
	Mem        float64 `json:"mem"`
	Disk       float64 `json:"disk"`
	Saturation float64 `json:"saturation"` // max(cpu, mem, disk)
	Tone       string  `json:"tone"`       // "ok" | "warn" | "err"

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

// hostMetricRow is one (host, metric) window-average. service.go folds the CPU /
// memory / disk families per host.
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

// subsystemForHost maps a host to a subsystem by name prefix. Host metrics carry
// no explicit subsystem label, so prefix-matching is the minimal viable signal
// (it matches the conventional kafka-broker-* / pg-* naming).
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
