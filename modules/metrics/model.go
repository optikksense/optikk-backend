package metrics

import "time"

// MetricFilters defines common query filters for all metric queries.
type MetricFilters struct {
	TeamUUID    string
	Start       time.Time
	End         time.Time
	ServiceName string // optional — empty means all services
}

// ServiceMetric represents aggregate metrics for a service.
type ServiceMetric struct {
	ServiceName  string  `json:"serviceName"`
	RequestCount int64   `json:"requestCount"`
	ErrorCount   int64   `json:"errorCount"`
	ErrorRate    float64 `json:"errorRate"`
	AvgLatency   float64 `json:"avgLatency"`
	P50Latency   float64 `json:"p50Latency,omitempty"`
	P95Latency   float64 `json:"p95Latency,omitempty"`
	P99Latency   float64 `json:"p99Latency,omitempty"`
	Status       string  `json:"status,omitempty"`
}

// EndpointMetric represents metrics for a specific endpoint.
type EndpointMetric struct {
	ServiceName   string  `json:"serviceName"`
	OperationName string  `json:"operationName"`
	HTTPMethod    string  `json:"httpMethod"`
	RequestCount  int64   `json:"requestCount"`
	ErrorCount    int64   `json:"errorCount"`
	AvgLatency    float64 `json:"avgLatency"`
	P50Latency    float64 `json:"p50Latency"`
	P95Latency    float64 `json:"p95Latency"`
	P99Latency    float64 `json:"p99Latency"`
}

// TimeSeriesPoint represents a single data point in a time series.
type TimeSeriesPoint struct {
	Timestamp     time.Time `json:"timestamp"`
	ServiceName   string    `json:"serviceName,omitempty"`
	OperationName string    `json:"operationName,omitempty"`
	HTTPMethod    string    `json:"httpMethod,omitempty"`
	RequestCount  int64     `json:"requestCount"`
	ErrorCount    int64     `json:"errorCount"`
	AvgLatency    float64   `json:"avgLatency"`
	P50           float64   `json:"p50,omitempty"`
	P95           float64   `json:"p95,omitempty"`
	P99           float64   `json:"p99,omitempty"`
}

// MetricsSummary represents a high-level summary of metrics.
type MetricsSummary struct {
	TotalRequests int64   `json:"totalRequests"`
	ErrorCount    int64   `json:"errorCount"`
	ErrorRate     float64 `json:"errorRate"`
	AvgLatency    float64 `json:"avgLatency"`
	P95Latency    float64 `json:"p95Latency"`
	P99Latency    float64 `json:"p99Latency"`
}

// TopologyNode represents a node in the service topology graph.
type TopologyNode struct {
	Name         string  `json:"name"`
	Status       string  `json:"status"`
	RequestCount int64   `json:"requestCount"`
	ErrorRate    float64 `json:"errorRate"`
	AvgLatency   float64 `json:"avgLatency"`
}

// TopologyEdge represents an edge in the service topology graph.
type TopologyEdge struct {
	Source     string  `json:"source"`
	Target     string  `json:"target"`
	CallCount  int64   `json:"callCount"`
	AvgLatency float64 `json:"avgLatency"`
	ErrorRate  float64 `json:"errorRate"`
}

// TopologyData represents the complete topology graph.
type TopologyData struct {
	Nodes []TopologyNode `json:"nodes"`
	Edges []TopologyEdge `json:"edges"`
}
