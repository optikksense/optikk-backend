package resource_utilisation

type MetricValue struct {
	Value float64 `json:"value"`
}

type ServiceResource struct {
	ServiceName           string   `json:"service_name"`
	AvgCpuUtil            *float64 `json:"avg_cpu_util"`
	AvgMemoryUtil         *float64 `json:"avg_memory_util"`
	AvgDiskUtil           *float64 `json:"avg_disk_util"`
	AvgNetworkUtil        *float64 `json:"avg_network_util"`
	AvgConnectionPoolUtil *float64 `json:"avg_connection_pool_util"`
	SampleCount           int64    `json:"sample_count"`
}

type InstanceResource struct {
	Host                  string   `json:"host"`
	Pod                   string   `json:"pod"`
	Container             string   `json:"container"`
	ServiceName           string   `json:"service_name"`
	AvgCpuUtil            *float64 `json:"avg_cpu_util"`
	AvgMemoryUtil         *float64 `json:"avg_memory_util"`
	AvgDiskUtil           *float64 `json:"avg_disk_util"`
	AvgNetworkUtil        *float64 `json:"avg_network_util"`
	AvgConnectionPoolUtil *float64 `json:"avg_connection_pool_util"`
	SampleCount           int64    `json:"sample_count"`
}

type ResourceBucket struct {
	Timestamp string   `json:"timestamp" ch:"time_bucket"`
	Pod       string   `json:"pod" ch:"pod"`
	Value     *float64 `json:"value" ch:"metric_val"`
}

type StateBucket struct {
	Timestamp string   `json:"timestamp" ch:"time_bucket"`
	State     string   `json:"state" ch:"state"`
	Value     *float64 `json:"value" ch:"metric_val"`
}

type DirectionBucket struct {
	Timestamp string   `json:"timestamp" ch:"time_bucket"`
	Direction string   `json:"direction" ch:"direction"`
	Value     *float64 `json:"value" ch:"metric_val"`
}

type MountpointBucket struct {
	Timestamp  string   `json:"timestamp" ch:"time_bucket"`
	Mountpoint string   `json:"mountpoint" ch:"mountpoint"`
	Value      *float64 `json:"value" ch:"metric_val"`
}
