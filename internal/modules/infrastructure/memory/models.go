package memory

// ---------------------------------------------------------------------------
// HTTP response DTOs (API contract).
// ---------------------------------------------------------------------------

type MetricValue struct {
	Value float64 `json:"value"`
}

// HostValue is one ranked top-consumer host with its blended memory
// utilization percentage.
type HostValue struct {
	Host  string  `json:"host"`
	Value float64 `json:"value"`
}

// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type MemoryMetricNameRow struct {
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}

type MemoryHostMetricRow struct {
	Host       string  `ch:"host"`
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}
