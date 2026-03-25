package topology

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *TopologyHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/services/topology", h.GetTopology)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &topologyModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type topologyModule struct {
	handler *TopologyHandler
}

func (m *topologyModule) Name() string                      { return "topology" }
func (m *topologyModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *topologyModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &TopologyHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *topologyModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
