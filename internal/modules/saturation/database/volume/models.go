package volume

import "time"

type OpsTimeSeries struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	GroupBy    string   `json:"group_by" ch:"group_by"`
	OpsPerSec  *float64 `json:"ops_per_sec" ch:"ops_per_sec"`
}

type opsRawDTO struct {
	TimeBucket time.Time `ch:"time_bucket"`
	GroupBy    string    `ch:"group_by"`
	OpsPerSec  float64   `ch:"ops_per_sec"`
}
