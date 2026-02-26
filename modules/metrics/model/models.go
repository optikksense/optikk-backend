package model

import "time"

// ServiceMetric represents aggregate metrics for a service.
type ServiceMetric struct {
	ServiceName  string  `json:"serviceName"`
	RequestCount int64   `json:"requestCount"`
	ErrorCount   int64   `json:"errorCount"`
	AvgLatency   float64 `json:"avgLatency"`
	P50Latency   float64 `json:"p50Latency,omitempty"`
	P95Latency   float64 `json:"p95Latency,omitempty"`
	P99Latency   float64 `json:"p99Latency,omitempty"`
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

// DashboardOverview represents the combined data for the dashboard overview.
type DashboardOverview struct {
	Metrics   MetricOverview `json:"metrics"`
	Logs      LogOverview    `json:"logs"`
	Traces    TraceOverview  `json:"traces"`
	TimeRange TimeRange      `json:"timeRange"`
}

type MetricOverview struct {
	Count      int         `json:"count"`
	Recent     []any       `json:"recent"`
	Statistics MetricStats `json:"statistics"`
}

type MetricStats struct {
	AvgLatency    float64 `json:"avgLatency"`
	TotalRequests int64   `json:"totalRequests"`
}

type LogOverview struct {
	Count       int              `json:"count"`
	Recent      []any            `json:"recent"`
	LevelCounts map[string]int64 `json:"levelCounts"`
}

type TraceOverview struct {
	Count        int              `json:"count"`
	Recent       []any            `json:"recent"`
	StatusCounts map[string]int64 `json:"statusCounts"`
}

type TimeRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// ServiceHealth represents the health status of a service.
type ServiceHealth struct {
	Name        string  `json:"name"`
	Status      string  `json:"status"`
	MetricCount int64   `json:"metricCount"`
	LogCount    int64   `json:"logCount"`
	TraceCount  int64   `json:"traceCount"`
	ErrorCount  int64   `json:"errorCount"`
	ErrorRate   float64 `json:"errorRate"`
	LastSeen    int64   `json:"lastSeen"`
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

// MetricsSummary represents a high-level summary of metrics.
type MetricsSummary struct {
	TotalRequests int64   `json:"totalRequests"`
	ErrorCount    int64   `json:"errorCount"`
	ErrorRate     float64 `json:"errorRate"`
	AvgLatency    float64 `json:"avgLatency"`
	P95Latency    float64 `json:"p95Latency"`
	P99Latency    float64 `json:"p99Latency"`
}
