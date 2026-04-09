package overview

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *OverviewHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/overview/request-rate", h.GetRequestRate)
	v1.GET("/overview/error-rate", h.GetErrorRate)
	v1.GET("/overview/p95-latency", h.GetP95Latency)
	v1.GET("/overview/services", h.GetServices)
	v1.GET("/overview/endpoints/metrics", h.GetTopEndpoints)
	v1.GET("/overview/summary", h.GetSummary)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &overviewModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type overviewModule struct {
	handler *OverviewHandler
}

func (m *overviewModule) Name() string                      { return "overview" }
func (m *overviewModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *overviewModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &OverviewHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *overviewModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
