package model

// InfrastructureSummary represents host/pod/container level resource summary.
type InfrastructureSummary struct {
	Host       string   `json:"host"`
	Pod        string   `json:"pod"`
	Container  string   `json:"container"`
	SpanCount  int64    `json:"spanCount"`
	ErrorCount int64    `json:"errorCount"`
	AvgLatency float64  `json:"avgLatency"`
	P95Latency float64  `json:"p95Latency"`
	Services   []string `json:"services"`
}

// InfrastructureNode represents host-level aggregation for the nodes view.
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

// InfrastructureNodeService represents services running on a specific host.
type InfrastructureNodeService struct {
	ServiceName  string  `json:"service_name"`
	RequestCount int64   `json:"request_count"`
	ErrorCount   int64   `json:"error_count"`
	ErrorRate    float64 `json:"error_rate"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	PodCount     int64   `json:"pod_count"`
}
