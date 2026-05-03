package cpu

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

type LoadAverageResult struct {
	Load1m  float64 `json:"load_1m"`
	Load5m  float64 `json:"load_5m"`
	Load15m float64 `json:"load_15m"`
}

type MetricValue struct {
	Value float64 `json:"value"`
}

type CPUServiceMetric struct {
	ServiceName string   `json:"service_name"`
	Value       *float64 `json:"value"`
}

type CPUInstanceMetric struct {
	Host        string   `json:"host"`
	Pod         string   `json:"pod"`
	Container   string   `json:"container"`
	ServiceName string   `json:"service_name"`
	Value       *float64 `json:"value"`
}

// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type CPUStateRow struct {
	Timestamp time.Time `ch:"timestamp"`
	State     string    `ch:"state"`
	Value     float64   `ch:"value"`
}

type CPUPodMetricRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	Pod        string    `ch:"pod"`
	MetricName string    `ch:"metric_name"`
	Value      float64   `ch:"value"`
}

type CPUMetricNameRow struct {
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}

type CPUServiceMetricRow struct {
	Service    string  `ch:"service"`
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}

type CPUInstanceMetricRow struct {
	Host       string  `ch:"host"`
	Pod        string  `ch:"pod"`
	Container  string  `ch:"container"`
	Service    string  `ch:"service"`
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}
