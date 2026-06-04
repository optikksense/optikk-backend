package memory

// ---------------------------------------------------------------------------
// HTTP response DTOs (API contract).
// ---------------------------------------------------------------------------

type MetricValue struct {
	Value float64 `json:"value"`
}

<<<<<<< HEAD

=======
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326
// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type MemoryMetricNameRow struct {
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}
<<<<<<< HEAD


=======
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326
