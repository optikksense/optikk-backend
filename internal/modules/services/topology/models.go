package topology

// Health classifies a service node for coloring on the frontend.
const (
	HealthHealthy   = "healthy"
	HealthDegraded  = "degraded"
	HealthUnhealthy = "unhealthy"
)

// ServiceNode is one service in the runtime topology.
type ServiceNode struct {
	Name         string  `json:"name"`
	RequestCount int64   `json:"request_count"`
	ErrorCount   int64   `json:"error_count"`
	ErrorRate    float64 `json:"error_rate"` // fraction in [0, 1]
	P50LatencyMs float64 `json:"p50_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	P99LatencyMs float64 `json:"p99_latency_ms"`
	Health       string  `json:"health"`
}

// ServiceEdge is a directed call relationship between two services.
type ServiceEdge struct {
	Source       string  `json:"source"`
	Target       string  `json:"target"`
	CallCount    int64   `json:"call_count"`
	ErrorCount   int64   `json:"error_count"`
	ErrorRate    float64 `json:"error_rate"`
	P50LatencyMs float64 `json:"p50_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
}

// TopologyResponse is the payload for GET /services/topology.
type TopologyResponse struct {
	Nodes []ServiceNode `json:"nodes"`
	Edges []ServiceEdge `json:"edges"`
}
