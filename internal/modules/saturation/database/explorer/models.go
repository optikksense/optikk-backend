package explorer

type DatastoreSystemRow struct {
	System            string  `json:"system"`
	Category          string  `json:"category"`
	QueryCount        int64   `json:"query_count"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	P95LatencyMs      float64 `json:"p95_latency_ms"`
	ErrorRate         float64 `json:"error_rate"`
	ActiveConnections int64   `json:"active_connections"`
	ServerHint        string  `json:"server_hint"`
	LastSeen          string  `json:"last_seen"`
}
