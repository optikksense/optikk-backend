package explorer

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

// NewModule creates the AI explorer module, following the standard module pattern.
func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	m := &aiExplorerModule{}
	m.configure(nativeQuerier, getTenant)
	return m
}

type aiExplorerModule struct {
	handler *Handler
}

func (m *aiExplorerModule) Name() string                      { return "aiExplorer" }
func (m *aiExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *aiExplorerModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	repo := NewRepository(nativeQuerier)
	svc := NewService(repo)
	m.handler = NewHandler(getTenant, svc)
}

func (m *aiExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	if m.handler == nil {
		return
	}
	group.POST("/ai/explorer/query", m.handler.Query)
	group.POST("/ai/explorer/sessions/query", m.handler.SessionsQuery)
}
