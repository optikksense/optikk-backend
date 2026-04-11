package ai

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	a := v1.Group("/ai")
	a.GET("/overview", h.GetOverview)
	a.GET("/overview/timeseries", h.GetOverviewTimeseries)
	a.GET("/overview/top-models", h.GetTopModels)
	a.GET("/overview/top-prompts", h.GetTopPrompts)
	a.GET("/overview/quality", h.GetQualitySummary)
	a.POST("/explorer/query", h.QueryRuns)
	a.GET("/runs/:runId", h.GetRunDetail)
	a.GET("/runs/:runId/related", h.GetRunRelated)

	v1.POST("/explorer/ai/analytics", h.Analytics)
}

type Module struct {
	handler *Handler
}

func NewModule(nativeQuerier *registry.NativeQuerier, sqlDB *registry.SQLDB, getTenant registry.GetTenantFunc) registry.Module {
	repo := NewRepository(nativeQuerier, sqlDB)
	return &Module{
		handler: NewHandler(getTenant, NewService(repo)),
	}
}

func (m *Module) Name() string                      { return "ai" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
