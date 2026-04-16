package runs

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

// NewModule creates the AI runs module.
func NewModule(deps *registry.Deps) (registry.Module, error) {
	repo := NewRepository(deps.NativeQuerier, deps.DB)
	svc := NewService(repo)
	return &runsModule{handler: NewHandler(deps.GetTenant, svc)}, nil
}

type runsModule struct {
	handler *Handler
}

func (m *runsModule) Name() string                      { return "aiRuns" }
func (m *runsModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *runsModule) RegisterRoutes(group *gin.RouterGroup) {
	a := group.Group("/ai")
	a.POST("/explorer/query", m.handler.QueryRuns)
	a.GET("/runs/:runId", m.handler.GetRunDetail)
	a.GET("/runs/:runId/related", m.handler.GetRunRelated)

	group.POST("/explorer/ai/analytics", m.handler.Analytics)
}
