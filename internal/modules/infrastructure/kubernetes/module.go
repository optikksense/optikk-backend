package kubernetes

import (
	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/registry"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

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

func init() {
	registry.Register(&kubernetesModule{})
}

type kubernetesModule struct {
	handler *KubernetesHandler
}

func (m *kubernetesModule) Name() string                      { return "kubernetes" }
func (m *kubernetesModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *kubernetesModule) Init(deps registry.Deps) error {
	m.handler = &KubernetesHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *kubernetesModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
