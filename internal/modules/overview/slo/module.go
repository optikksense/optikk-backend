package slo

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *SLOHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/overview/slo", h.GetSloSli)
	v1.GET("/overview/slo/stats", h.GetSloStats)
	v1.GET("/overview/slo/burn-down", h.GetBurnDown)
	v1.GET("/overview/slo/burn-rate", h.GetBurnRate)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &overviewSLOModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type overviewSLOModule struct {
	handler *SLOHandler
}

func (m *overviewSLOModule) Name() string                      { return "overviewSLO" }
func (m *overviewSLOModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *overviewSLOModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &SLOHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *overviewSLOModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
