package disk

import "time"

// ---------------------------------------------------------------------------
// HTTP response DTOs (API contract).
// ---------------------------------------------------------------------------

type DirectionBucket struct {
	Timestamp string   `json:"timestamp"`
	Direction string   `json:"direction"`
	Value     *float64 `json:"value"`
}

type ResourceBucket struct {
	Timestamp string   `json:"timestamp"`
	Pod       string   `json:"pod"`
	Value     *float64 `json:"value"`
}

type MountpointBucket struct {
	Timestamp  string   `json:"timestamp"`
	Mountpoint string   `json:"mountpoint"`
	Value      *float64 `json:"value"`
}

type MetricValue struct {
	Value float64 `json:"value"`
}

type DiskServiceMetric struct {
	ServiceName string   `json:"service_name"`
	AvgDiskUtil *float64 `json:"avg_disk_util"`
	SampleCount int64    `json:"sample_count"`
}

type DiskInstanceMetric struct {
	Host        string   `json:"host"`
	Pod         string   `json:"pod"`
	Container   string   `json:"container"`
	ServiceName string   `json:"service_name"`
	AvgDiskUtil *float64 `json:"avg_disk_util"`
	SampleCount int64    `json:"sample_count"`
}

// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type DiskDirectionRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Direction string    `ch:"direction"`
	Value     float64   `ch:"value"`
}

type DiskValueRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Value     float64   `ch:"value"`
}

type DiskMountpointRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	Mountpoint string    `ch:"mountpoint"`
	Value      float64   `ch:"value"`
}

type DiskMetricNameRow struct {
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}
