package fleet

import "time"

// FleetPod aggregates root-span traffic by Kubernetes pod name and host.
type FleetPod struct {
	PodName      string   `json:"pod_name"`
	Host         string   `json:"host"`
	Services     []string `json:"services"`
	RequestCount int64    `json:"request_count"`
	ErrorCount   int64    `json:"error_count"`
	ErrorRate    float64  `json:"error_rate"`
	AvgLatencyMs float64  `json:"avg_latency_ms"`
	P95LatencyMs float64  `json:"p95_latency_ms"`
	LastSeen     string   `json:"last_seen"`
}

// FleetPodAggregateRow is the raw row scanned from CH; service computes
// derived fields (error_rate, avg_latency_ms) and constructs the DTO.
type FleetPodAggregateRow struct {
	Pod           string    `ch:"pod"`
	Host          string    `ch:"host"`
	Services      []string  `ch:"services"`
	RequestCount  uint64    `ch:"request_count"`
	ErrorCount    uint64    `ch:"error_count"`
	DurationMsSum float64   `ch:"duration_ms_sum"`
	P95LatencyMs  float32   `ch:"p95_latency_ms"`
	LastSeen      time.Time `ch:"last_seen"`
}
