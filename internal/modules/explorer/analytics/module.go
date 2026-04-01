package analytics

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

// NewModule creates the explorer analytics module.
func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	return &explorerAnalyticsModule{
		handler: NewHandler(getTenant, NewService(nativeQuerier)),
	}
}

type explorerAnalyticsModule struct {
	handler *Handler
}

func (m *explorerAnalyticsModule) Name() string                      { return "explorerAnalytics" }
func (m *explorerAnalyticsModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *explorerAnalyticsModule) RegisterRoutes(group *gin.RouterGroup) {
	group.POST("/explorer/:scope/analytics", m.handler.Query)
}
