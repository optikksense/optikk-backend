package memory

import "time"

// ---------------------------------------------------------------------------
// HTTP response DTOs (API contract).
// ---------------------------------------------------------------------------

type StateBucket struct {
	Timestamp string   `json:"timestamp"`
	State     string   `json:"state"`
	Value     *float64 `json:"value"`
}

type ResourceBucket struct {
	Timestamp string   `json:"timestamp"`
	Pod       string   `json:"pod"`
	Value     *float64 `json:"value"`
}

type MetricValue struct {
	Value float64 `json:"value"`
}

type MemoryServiceMetric struct {
	ServiceName   string   `json:"service_name"`
	AvgMemoryUtil *float64 `json:"avg_memory_util"`
	SampleCount   int64    `json:"sample_count"`
}

type MemoryInstanceMetric struct {
	Host          string   `json:"host"`
	Pod           string   `json:"pod"`
	Container     string   `json:"container"`
	ServiceName   string   `json:"service_name"`
	AvgMemoryUtil *float64 `json:"avg_memory_util"`
	SampleCount   int64    `json:"sample_count"`
}

// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type MemoryStateRow struct {
	Timestamp time.Time `ch:"timestamp"`
	State     string    `ch:"state"`
	Value     float64   `ch:"value"`
}

type MemoryPodMetricRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	Pod        string    `ch:"pod"`
	MetricName string    `ch:"metric_name"`
	Value      float64   `ch:"value"`
}

type MemoryMetricNameRow struct {
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}
