package resource_utilisation

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ResourceUtilisationHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	g := v1.Group("/infrastructure/resource-utilisation")
	g.GET("/avg-cpu", h.GetAvgCPU)
	g.GET("/avg-memory", h.GetAvgMemory)
	g.GET("/avg-network", h.GetAvgNetwork)
	g.GET("/avg-conn-pool", h.GetAvgConnPool)
	g.GET("/cpu-usage-percentage", h.GetCPUUsagePercentage)
	g.GET("/memory-usage-percentage", h.GetMemoryUsagePercentage)
	g.GET("/by-service", h.GetByService)
	g.GET("/by-instance", h.GetByInstance)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &resourceUtilisationModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type resourceUtilisationModule struct {
	handler *ResourceUtilisationHandler
}

func (m *resourceUtilisationModule) Name() string                      { return "resourceUtilisation" }
func (m *resourceUtilisationModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *resourceUtilisationModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &ResourceUtilisationHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *resourceUtilisationModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
