package nodes

import "time"

// ---------------------------------------------------------------------------
// HTTP response DTOs (API contract).
// ---------------------------------------------------------------------------

type InfrastructureNode struct {
	Host           string   `json:"host"`
	PodCount       int64    `json:"pod_count"`
	ContainerCount int64    `json:"container_count"`
	Services       []string `json:"services"`
	RequestCount   int64    `json:"request_count"`
	ErrorCount     int64    `json:"error_count"`
	ErrorRate      float64  `json:"error_rate"`
	AvgLatencyMs   float64  `json:"avg_latency_ms"`
	P95LatencyMs   float64  `json:"p95_latency_ms"`
	LastSeen       string   `json:"last_seen"`
}

type InfrastructureNodeService struct {
	ServiceName  string  `json:"service_name"`
	RequestCount int64   `json:"request_count"`
	ErrorCount   int64   `json:"error_count"`
	ErrorRate    float64 `json:"error_rate"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	PodCount     int64   `json:"pod_count"`
}

type InfrastructureNodeSummary struct {
	HealthyNodes   int64 `json:"healthy_nodes"`
	DegradedNodes  int64 `json:"degraded_nodes"`
	UnhealthyNodes int64 `json:"unhealthy_nodes"`
	TotalPods      int64 `json:"total_pods"`
}

// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type NodeAggregateRow struct {
	Host          string    `ch:"host"`
	PodCount      uint64    `ch:"pod_count"`
	RequestCount  uint64    `ch:"request_count"`
	ErrorCount    uint64    `ch:"error_count"`
	DurationMsSum float64   `ch:"duration_ms_sum"`
	P95LatencyMs  float32   `ch:"p95_latency_ms"`
	LastSeen      time.Time `ch:"last_seen"`
}

type NodeServiceAggregateRow struct {
	Service       string  `ch:"service"`
	RequestCount  uint64  `ch:"request_count"`
	ErrorCount    uint64  `ch:"error_count"`
	DurationMsSum float64 `ch:"duration_ms_sum"`
	P95LatencyMs  float32 `ch:"p95_latency_ms"`
	PodCount      uint64  `ch:"pod_count"`
}

type NodeSummaryRow struct {
	HealthyNodes   int64  `ch:"healthy_nodes"`
	DegradedNodes  int64  `ch:"degraded_nodes"`
	UnhealthyNodes int64  `ch:"unhealthy_nodes"`
	TotalPods      *int64 `ch:"total_pods"`
}
