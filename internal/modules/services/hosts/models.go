package hosts

import "time"

// HostStatus is the health classification for one host running a service.
// Mirrors the dot-color scheme used by the design (healthy / warn / error).
type HostStatus string

const (
	HostHealthy HostStatus = "healthy"
	HostWarn    HostStatus = "warn"
	HostError   HostStatus = "error"
)

// HostForService is one card on the Service Detail "Hosts" grid.
type HostForService struct {
	Host         string     `json:"host"`
	Zone         string     `json:"zone"`
	CPUPct       *float64   `json:"cpu_pct,omitempty"`
	MemPct       *float64   `json:"mem_pct,omitempty"`
	RPS          float64    `json:"rps"`
	ErrorRate    float64    `json:"error_rate"`
	P99Ms        float64    `json:"p99_ms"`
	Status       HostStatus `json:"status"`
	LastSeen     string     `json:"last_seen"`
	RequestCount int64      `json:"request_count"`
	ErrorCount   int64      `json:"error_count"`
}

// hostSpansRow is the per-host aggregate read from spans_1m.
type hostSpansRow struct {
	Host         string    `ch:"host"`
	Zone         string    `ch:"zone"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
	P99Ms        float32   `ch:"p99_ms"`
	LastSeen     time.Time `ch:"last_seen"`
}

// hostMetricRow is one per-host CPU or mem average from metrics_1m.
type hostMetricRow struct {
	Host  string  `ch:"host"`
	Value float64 `ch:"value"`
}
