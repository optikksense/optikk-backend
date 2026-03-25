package servicepage

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ServiceHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/services/summary/total", h.GetTotalServices)
	v1.GET("/services/summary/healthy", h.GetHealthyServices)
	v1.GET("/services/summary/degraded", h.GetDegradedServices)
	v1.GET("/services/summary/unhealthy", h.GetUnhealthyServices)
	v1.GET("/services/metrics", h.GetServiceMetrics)
	v1.GET("/services/timeseries", h.GetServiceTimeSeries)
	v1.GET("/services/:serviceName/endpoints", h.GetServiceEndpoints)
	v1.GET("/services/navigator", h.GetNavigator)

	// Legacy path alias for backward compat with the metrics feature page.
	v1.GET("/metrics/timeseries", h.GetServiceTimeSeries)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &servicePageModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type servicePageModule struct {
	handler *ServiceHandler
}

func (m *servicePageModule) Name() string                      { return "servicePage" }
func (m *servicePageModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *servicePageModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &ServiceHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *servicePageModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
