package explorer

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// NewModule creates the AI explorer registry.Module.
func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	m := &aiExplorerModule{}
	m.configure(nativeQuerier, getTenant)
	return m
}

type aiExplorerModule struct {
	handler *Handler
}

func (m *aiExplorerModule) Name() string                      { return "aiExplorer" }
func (m *aiExplorerModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *aiExplorerModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *aiExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	ai := group.Group("/ai/explorer")
	ai.GET("/spans", m.handler.GetSpans)
	ai.GET("/facets", m.handler.GetFacets)
	ai.GET("/summary", m.handler.GetSummary)
	ai.GET("/histogram", m.handler.GetHistogram)
}
