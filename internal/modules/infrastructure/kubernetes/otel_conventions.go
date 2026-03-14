package kubernetes

import "fmt"

// OpenTelemetry Semantic Conventions for Kubernetes / Container Metrics
// Reference: https://opentelemetry.io/docs/specs/semconv/k8s/

const (
	MetricContainerCPUTime          = "container.cpu.time"
	MetricContainerCPUThrottledTime = "container.cpu.throttling_data.throttled_time"
	MetricContainerMemoryUsage      = "container.memory.usage"
	MetricContainerOOMKillCount     = "container.memory.oom_kill_count"
	MetricK8sContainerRestarts      = "k8s.container.restarts"
	MetricK8sNodeAllocatableCPU     = "k8s.node.allocatable.cpu"
	MetricK8sNodeAllocatableMemory  = "k8s.node.allocatable.memory"
	MetricK8sPodPhase               = "k8s.pod.phase"
	MetricK8sReplicaSetDesired      = "k8s.replicaset.desired"
	MetricK8sReplicaSetAvailable    = "k8s.replicaset.available"
	MetricK8sVolumeCapacity         = "k8s.volume.capacity"
	MetricK8sVolumeInodes           = "k8s.volume.inodes"

	AttrContainerName  = "container.name"
	AttrK8sPodName     = "k8s.pod.name"
	AttrK8sNamespace   = "k8s.namespace.name"
	AttrK8sPodPhase    = "k8s.pod.phase"
	AttrK8sNodeName    = "k8s.node.name"
	AttrK8sVolumeName  = "k8s.volume.name"
	AttrReplicaSetName = "k8s.replicaset.name"

	TableMetrics = "observability.metrics"

	ColMetricName = "metric_name"
	ColTeamID     = "team_id"
	ColTimestamp  = "timestamp"
	ColValue      = "value"
)

func attrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}
