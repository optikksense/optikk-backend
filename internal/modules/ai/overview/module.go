package overview

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

// NewModule creates the AI overview module.
func NewModule(deps *registry.Deps) (registry.Module, error) {
	repo := NewRepository(deps.NativeQuerier)
	svc := NewService(repo)
	return &overviewModule{handler: NewHandler(deps.GetTenant, svc)}, nil
}

type overviewModule struct {
	handler *Handler
}

func (m *overviewModule) Name() string                      { return "aiOverview" }
func (m *overviewModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *overviewModule) RegisterRoutes(group *gin.RouterGroup) {
	a := group.Group("/ai")
	a.GET("/overview", m.handler.GetOverview)
	a.GET("/overview/timeseries", m.handler.GetOverviewTimeseries)
	a.GET("/overview/top-models", m.handler.GetTopModels)
	a.GET("/overview/top-prompts", m.handler.GetTopPrompts)
	a.GET("/overview/quality", m.handler.GetQualitySummary)
}
