package explorer

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

// NewModule creates the AI explorer module, following the standard module pattern.
func NewModule(deps *registry.Deps) (registry.Module, error) {
	m := &aiExplorerModule{}
	m.configure(deps)
	return m, nil
}

type aiExplorerModule struct {
	handler *Handler
}

func (m *aiExplorerModule) Name() string                      { return "aiExplorer" }
func (m *aiExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *aiExplorerModule) configure(deps *registry.Deps) {
	repo := NewRepository(deps.NativeQuerier)
	svc := NewService(repo)
	m.handler = NewHandler(deps.GetTenant, svc)
}

func (m *aiExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	if m.handler == nil {
		return
	}
	group.POST("/ai/explorer/query", m.handler.Query)
	group.POST("/ai/explorer/sessions/query", m.handler.SessionsQuery)
}
