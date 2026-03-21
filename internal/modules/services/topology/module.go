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

func init() {
	registry.Register(&topologyModule{})
}

type topologyModule struct {
	handler *TopologyHandler
}

func (m *topologyModule) Name() string                      { return "topology" }
func (m *topologyModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *topologyModule) Init(deps registry.Deps) error {
	m.handler = &TopologyHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *topologyModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
