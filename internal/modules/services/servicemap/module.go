package servicemap

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ServiceMapHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/services/topology", h.GetTopology)
	v1.GET("/services/:serviceName/upstream-downstream", h.GetUpstreamDownstream)
	v1.GET("/services/external-dependencies", h.GetExternalDependencies)
	v1.GET("/spans/client-server-latency", h.GetClientServerLatency)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &serviceMapModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type serviceMapModule struct {
	handler *ServiceMapHandler
}

func (m *serviceMapModule) Name() string                      { return "serviceMap" }
func (m *serviceMapModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *serviceMapModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &ServiceMapHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *serviceMapModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
