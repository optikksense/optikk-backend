package slo

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *SLOHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/overview/slo", h.GetSloSli)
	v1.GET("/overview/slo/stats", h.GetSloStats)
	v1.GET("/overview/slo/burn-down", h.GetBurnDown)
	v1.GET("/overview/slo/burn-rate", h.GetBurnRate)
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	return &overviewSLOModule{
		handler: &SLOHandler{
			DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
			Service:  NewService(NewRepository(deps.NativeQuerier)),
		},
	}, nil
}

type overviewSLOModule struct {
	handler *SLOHandler
}

func (m *overviewSLOModule) Name() string                      { return "overviewSLO" }
func (m *overviewSLOModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *overviewSLOModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
