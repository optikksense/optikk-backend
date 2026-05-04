package volume

type OpsTimeSeries struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	GroupBy    string   `json:"group_by" ch:"group_by"`
	OpsPerSec  *float64 `json:"ops_per_sec" ch:"ops_per_sec"`
}

type opsRawDTO struct {
	TimeBucket string  `ch:"time_bucket"`
	GroupBy    string  `ch:"group_by"`
	OpsPerSec  float64 `ch:"ops_per_sec"`
}

type ReadWritePoint struct {
	TimeBucket     string   `json:"time_bucket" ch:"time_bucket"`
	ReadOpsPerSec  *float64 `json:"read_ops_per_sec" ch:"read_ops_per_sec"`
	WriteOpsPerSec *float64 `json:"write_ops_per_sec" ch:"write_ops_per_sec"`
}

type readWriteRawDTO struct {
	TimeBucket    string  `ch:"time_bucket"`
	ReadOpsPerSec float64 `ch:"read_ops_per_sec"`
	WriteOpsPerSec float64 `ch:"write_ops_per_sec"`
}
