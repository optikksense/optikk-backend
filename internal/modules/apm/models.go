package apm

// HistogramSummary holds p50/p95/p99/avg for histogram metrics.
type HistogramSummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Avg float64 `json:"avg"`
}

// StateBucket represents timeseries data grouped by a state attribute.
type StateBucket struct {
	Timestamp string   `json:"timestamp"`
	State     string   `json:"state"`
	Value     *float64 `json:"value"`
}

// TimeBucket represents a simple scalar timeseries point.
type TimeBucket struct {
	Timestamp string   `json:"timestamp"`
	Value     *float64 `json:"value"`
}

// ProcessMemory holds resident set size and virtual memory size.
type ProcessMemory struct {
	RSS float64 `json:"rss"`
	VMS float64 `json:"vms"`
}
