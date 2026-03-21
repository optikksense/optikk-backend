package nodes

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *NodeHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/infrastructure/nodes", h.GetInfrastructureNodes)
	v1.GET("/infrastructure/nodes/summary", h.GetInfrastructureNodeSummary)
	v1.GET("/infrastructure/nodes/:host/services", h.GetInfrastructureNodeServices)
}

func init() {
	registry.Register(&nodesModule{})
}

type nodesModule struct {
	handler *NodeHandler
}

func (m *nodesModule) Name() string                      { return "nodes" }
func (m *nodesModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *nodesModule) Init(deps registry.Deps) error {
	m.handler = &NodeHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *nodesModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
