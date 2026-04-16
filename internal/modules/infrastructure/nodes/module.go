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

func NewModule(deps *registry.Deps) (registry.Module, error) {
	module := &nodesModule{}
	module.configure(deps)
	return module, nil
}

type nodesModule struct {
	handler *NodeHandler
}

func (m *nodesModule) Name() string                      { return "nodes" }
func (m *nodesModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *nodesModule) configure(deps *registry.Deps) {
	m.handler = &NodeHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
}

func (m *nodesModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
