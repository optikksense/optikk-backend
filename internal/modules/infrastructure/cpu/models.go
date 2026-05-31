package cpu

// ---------------------------------------------------------------------------
// HTTP response DTOs (API contract).
// ---------------------------------------------------------------------------

type MetricValue struct {
	Value float64 `json:"value"`
}

type CPUInstanceMetric struct {
	Host        string   `json:"host"`
	Pod         string   `json:"pod"`
	Container   string   `json:"container"`
	ServiceName string   `json:"service_name"`
	Value       *float64 `json:"value"`
}

// HostValue is one ranked top-consumer host with its blended CPU utilization
// percentage.
type HostValue struct {
	Host  string  `json:"host"`
	Value float64 `json:"value"`
}

// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type CPUMetricNameRow struct {
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

type CPUHostMetricRow struct {
	Host       string  `ch:"host"`
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}
