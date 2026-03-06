package kubernetes

import "github.com/gin-gonic/gin"

// Config holds Kubernetes module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts Kubernetes metric routes.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *KubernetesHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	k8s := v1.Group("/infrastructure/kubernetes")
	k8s.GET("/container-cpu", h.GetContainerCPU)
	k8s.GET("/cpu-throttling", h.GetCPUThrottling)
	k8s.GET("/container-memory", h.GetContainerMemory)
	k8s.GET("/oom-kills", h.GetOOMKills)
	k8s.GET("/pod-restarts", h.GetPodRestarts)
	k8s.GET("/node-allocatable", h.GetNodeAllocatable)
	k8s.GET("/pod-phases", h.GetPodPhases)
	k8s.GET("/replica-status", h.GetReplicaStatus)
	k8s.GET("/volume-usage", h.GetVolumeUsage)
}
