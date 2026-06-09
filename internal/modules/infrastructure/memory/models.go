package memory

// HTTP response DTOs.

type MetricValue struct {
	Value float64 `json:"value"`
}

// Internal repository row types.

type MemoryMetricNameRow struct {
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}
