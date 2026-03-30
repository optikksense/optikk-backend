package servicemap

import "time"

// serviceDependencyRow is the scan target for GetUpstreamDownstream.
// Direction ("upstream"/"downstream") is computed in the service layer.
type serviceDependencyRow struct {
	Source       string  `ch:"source"`
	Target       string  `ch:"target"`
	CallCount    int64   `ch:"call_count"`
	P95LatencyMs float64 `ch:"p95_latency_ms"`
	ErrorRate    float64 `ch:"error_rate"`
}

type topologyNodeRow struct {
	Name         string  `ch:"service_name"`
	RequestCount int64   `ch:"request_count"`
	ErrorCount   int64   `ch:"error_count"`
	AvgLatency   float64 `ch:"avg_latency"`
}

// clientServerLatencyRow is the scan target for GetClientServerLatency.
// NetworkGapMs = ClientP95Ms - ServerP95Ms is computed in the service layer.
type clientServerLatencyRow struct {
	Timestamp     time.Time `ch:"time_bucket"`
	OperationName string    `ch:"operation_name"`
	ClientP95Ms   float64   `ch:"client_p95_ms"`
	ServerP95Ms   float64   `ch:"server_p95_ms"`
}
