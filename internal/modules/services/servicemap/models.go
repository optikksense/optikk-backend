package servicemap

import "time"

// ServiceDependencyDetail has Direction computed in the service layer (not from DB).
type ServiceDependencyDetail struct {
	Source       string  `json:"source"`
	Target       string  `json:"target"`
	CallCount    int64   `json:"call_count"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	ErrorRate    float64 `json:"error_rate"`
	Direction    string  `json:"direction"` // "upstream" | "downstream" — computed in service
}

type ExternalDependency struct {
	SourceService string  `json:"source_service" ch:"source_service"`
	ExternalHost  string  `json:"external_host"  ch:"external_host"`
	CallCount     int64   `json:"call_count"     ch:"call_count"`
	P95LatencyMs  float64 `json:"p95_latency_ms" ch:"p95_latency_ms"`
	ErrorRate     float64 `json:"error_rate"     ch:"error_rate"`
}

// ClientServerLatencyPoint has NetworkGapMs computed in the service layer.
type ClientServerLatencyPoint struct {
	Timestamp     time.Time `json:"timestamp"`
	OperationName string    `json:"operation_name"`
	ClientP95Ms   float64   `json:"client_p95_ms"`
	ServerP95Ms   float64   `json:"server_p95_ms"`
	NetworkGapMs  float64   `json:"network_gap_ms"` // computed in service
}
