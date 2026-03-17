package topology

// topologyNodeRow is the scan target for the nodes query.
// Status is computed in the service layer from ErrorRate.
type topologyNodeRow struct {
	Name         string  `ch:"service_name"`
	RequestCount int64   `ch:"request_count"`
	ErrorCount   int64   `ch:"error_count"`
	AvgLatency   float64 `ch:"avg_latency"`
}
