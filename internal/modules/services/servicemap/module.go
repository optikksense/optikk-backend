package servicemap

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ServiceMapHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/services/:serviceName/upstream-downstream", h.GetUpstreamDownstream)
	v1.GET("/services/external-dependencies", h.GetExternalDependencies)
	v1.GET("/spans/client-server-latency", h.GetClientServerLatency)
}

func init() {
	registry.Register(&serviceMapModule{})
}

type serviceMapModule struct {
	handler *ServiceMapHandler
}

func (m *serviceMapModule) Name() string                      { return "serviceMap" }
func (m *serviceMapModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *serviceMapModule) Init(deps registry.Deps) error {
	m.handler = &ServiceMapHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *serviceMapModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
