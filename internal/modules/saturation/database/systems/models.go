package systems

import "time"

type DetectedSystem struct {
	DBSystem      string  `json:"db_system"`
	SpanCount     int64   `json:"span_count"`
	ErrorCount    int64   `json:"error_count"`
	AvgLatencyMs  float64 `json:"avg_latency_ms"`
	QueryCount    int64   `json:"query_count"`
	ServerAddress string  `json:"server_address"`
	LastSeen      string  `json:"last_seen"`
}

type detectedSystemDTO struct {
	DBSystem      string    `ch:"db_system"`
	SpanCount     int64     `ch:"span_count"`
	ErrorCount    int64     `ch:"error_count"`
	AvgLatencyMs  float64   `ch:"avg_latency_ms"`
	QueryCount    int64     `ch:"query_count"`
	ServerAddress string    `ch:"server_address"`
	LastSeen      time.Time `ch:"last_seen"`
}
