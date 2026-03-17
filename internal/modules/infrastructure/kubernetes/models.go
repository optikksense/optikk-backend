package kubernetes

type ContainerBucket struct {
	Timestamp string   `json:"timestamp" ch:"time_bucket"`
	Container string   `json:"container" ch:"container"`
	Value     *float64 `json:"value" ch:"val"`
}

type PodStat struct {
	PodName   string `json:"pod_name" ch:"pod_name"`
	Namespace string `json:"namespace" ch:"namespace"`
	Restarts  int64  `json:"restarts" ch:"restarts"`
}

type NodeAllocatable struct {
	CPUCores    float64 `json:"cpu_cores" ch:"cpu_cores"`
	MemoryBytes float64 `json:"memory_bytes" ch:"memory_bytes"`
}

type PhaseStat struct {
	Phase string `json:"phase" ch:"phase"`
	Count int64  `json:"count" ch:"pod_count"`
}

type ReplicaStat struct {
	ReplicaSet string `json:"replica_set" ch:"replica_set"`
	Desired    int64  `json:"desired" ch:"desired"`
	Available  int64  `json:"available" ch:"available"`
}

type VolumeStat struct {
	VolumeName    string  `json:"volume_name" ch:"volume_name"`
	CapacityBytes float64 `json:"capacity_bytes" ch:"capacity_bytes"`
	Inodes        int64   `json:"inodes" ch:"inodes"`
}
