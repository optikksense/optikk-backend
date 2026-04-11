package fleet

// FleetPod aggregates root-span traffic by Kubernetes pod name and host.
type FleetPod struct {
	PodName        string   `json:"pod_name"`
	Host           string   `json:"host"`
	Services       []string `json:"services"`
	RequestCount   int64    `json:"request_count"`
	ErrorCount     int64    `json:"error_count"`
	ErrorRate      float64  `json:"error_rate"`
	AvgLatencyMs float64  `json:"avg_latency_ms"`
	P95LatencyMs float64  `json:"p95_latency_ms"`
	LastSeen       string   `json:"last_seen"`
}
