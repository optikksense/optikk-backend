package nodes

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *NodeHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/infrastructure/nodes", h.GetInfrastructureNodes)
	v1.GET("/infrastructure/nodes/summary", h.GetInfrastructureNodeSummary)
	v1.GET("/infrastructure/nodes/:host/services", h.GetInfrastructureNodeServices)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &nodesModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type nodesModule struct {
	handler *NodeHandler
}

func (m *nodesModule) Name() string                      { return "nodes" }
func (m *nodesModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *nodesModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &NodeHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *nodesModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
