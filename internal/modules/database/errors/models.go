package errors

type ErrorTimeSeries struct {
	TimeBucket   string   `json:"time_bucket" ch:"time_bucket"`
	GroupBy      string   `json:"group_by" ch:"group_by"`
	ErrorsPerSec *float64 `json:"errors_per_sec" ch:"errors_per_sec"`
}

type ErrorRatioPoint struct {
	TimeBucket    string   `json:"time_bucket" ch:"time_bucket"`
	ErrorRatioPct *float64 `json:"error_ratio_pct" ch:"error_ratio_pct"`
}
