package anomaly

// BaselinePoint represents a single time-bucket with its current value,
// historical baseline, and anomaly classification.
type BaselinePoint struct {
	Timestamp     string  `json:"timestamp"`
	Value         float64 `json:"value"`
	BaselineValue float64 `json:"baseline_value"`
	UpperBound    float64 `json:"upper_bound"`
	LowerBound    float64 `json:"lower_bound"`
	IsAnomaly     bool    `json:"is_anomaly"`
}

// BaselineRequest holds the parsed query parameters for the baseline endpoint.
type BaselineRequest struct {
	Metric      string
	ServiceName string
	Sensitivity float64 // number of standard deviations for bounds (default 2.0)
}
