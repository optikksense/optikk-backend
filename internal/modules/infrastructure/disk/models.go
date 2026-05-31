package disk

// ---------------------------------------------------------------------------
// HTTP response DTOs (API contract).
// ---------------------------------------------------------------------------

type MetricValue struct {
	Value float64 `json:"value"`
}

// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type DiskMetricNameRow struct {
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}
