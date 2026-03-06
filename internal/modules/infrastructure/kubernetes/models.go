package kubernetes

// ContainerBucket represents timeseries data for a container metric.
type ContainerBucket struct {
	Timestamp string   `json:"timestamp"`
	Container string   `json:"container"`
	Value     *float64 `json:"value"`
}

// PodStat represents restart counts per pod.
type PodStat struct {
	PodName   string `json:"pod_name"`
	Namespace string `json:"namespace"`
	Restarts  int64  `json:"restarts"`
}

// NodeAllocatable holds the allocatable CPU and memory for nodes.
type NodeAllocatable struct {
	CPUCores    float64 `json:"cpu_cores"`
	MemoryBytes float64 `json:"memory_bytes"`
}

// PhaseStat represents the count of pods in each lifecycle phase.
type PhaseStat struct {
	Phase string `json:"phase"`
	Count int64  `json:"count"`
}

// ReplicaStat represents desired vs available replicas per ReplicaSet.
type ReplicaStat struct {
	ReplicaSet string `json:"replica_set"`
	Desired    int64  `json:"desired"`
	Available  int64  `json:"available"`
}

// VolumeStat holds volume capacity and inode info.
type VolumeStat struct {
	VolumeName    string  `json:"volume_name"`
	CapacityBytes float64 `json:"capacity_bytes"`
	Inodes        int64   `json:"inodes"`
}
