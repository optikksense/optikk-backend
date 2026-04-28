package network

import "time"

// ---------------------------------------------------------------------------
// HTTP response DTOs (API contract).
// ---------------------------------------------------------------------------

type DirectionBucket struct {
	Timestamp string   `json:"timestamp"`
	Direction string   `json:"direction"`
	Value     *float64 `json:"value"`
}

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

type NetworkServiceMetric struct {
	ServiceName    string   `json:"service_name"`
	AvgNetworkUtil *float64 `json:"avg_network_util"`
	SampleCount    int64    `json:"sample_count"`
}

type NetworkInstanceMetric struct {
	Host           string   `json:"host"`
	Pod            string   `json:"pod"`
	Container      string   `json:"container"`
	ServiceName    string   `json:"service_name"`
	AvgNetworkUtil *float64 `json:"avg_network_util"`
	SampleCount    int64    `json:"sample_count"`
}

// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type NetworkDirectionRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Direction string    `ch:"direction"`
	Value     float64   `ch:"value"`
}

type NetworkStateRow struct {
	Timestamp time.Time `ch:"timestamp"`
	State     string    `ch:"state"`
	Value     float64   `ch:"value"`
}

type NetworkValueRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Value     float64   `ch:"value"`
}

type NetworkServiceRow struct {
	Service string  `ch:"service"`
	Value   float64 `ch:"value"`
}

type NetworkScalarRow struct {
	Value float64 `ch:"value"`
}
