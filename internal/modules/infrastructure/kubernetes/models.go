package kubernetes

import "time"

// ---------------------------------------------------------------------------
// HTTP response DTOs (API contract).
// ---------------------------------------------------------------------------

type ContainerBucket struct {
	Timestamp string   `json:"timestamp"`
	Container string   `json:"container"`
	Value     *float64 `json:"value"`
}

type PodStat struct {
	PodName   string `json:"pod_name"`
	Namespace string `json:"namespace"`
	Restarts  int64  `json:"restarts"`
}

type NodeAllocatable struct {
	CPUCores    float64 `json:"cpu_cores"`
	MemoryBytes float64 `json:"memory_bytes"`
}

type PhaseStat struct {
	Phase string `json:"phase"`
	Count int64  `json:"count"`
}

type ReplicaStat struct {
	ReplicaSet string `json:"replica_set"`
	Desired    int64  `json:"desired"`
	Available  int64  `json:"available"`
}

type VolumeStat struct {
	VolumeName    string  `json:"volume_name"`
	CapacityBytes float64 `json:"capacity_bytes"`
	Inodes        int64   `json:"inodes"`
}

// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type ContainerRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Container string    `ch:"container"`
	Value     float64   `ch:"value"`
}

type PodRestartRow struct {
	Pod       string `ch:"pod"`
	Namespace string `ch:"namespace"`
	Restarts  int64  `ch:"restarts"`
}

type K8sMetricNameRow struct {
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}

type PhaseRow struct {
	Phase string `ch:"phase"`
	Count int64  `ch:"pod_count"`
}

type ReplicaSetRow struct {
	ReplicaSet string  `ch:"replica_set"`
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}

type VolumeRow struct {
	VolumeName string  `ch:"volume_name"`
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}
