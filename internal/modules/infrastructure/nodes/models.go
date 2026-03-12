package nodes

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

// InfrastructureNodeSummary captures aggregate node counts for dashboard stat cards.
type InfrastructureNodeSummary struct {
	HealthyNodes   int64 `json:"healthy_nodes"`
	DegradedNodes  int64 `json:"degraded_nodes"`
	UnhealthyNodes int64 `json:"unhealthy_nodes"`
	TotalPods      int64 `json:"total_pods"`
}
