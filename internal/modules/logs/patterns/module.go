package patterns

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	return &logPatternsModule{
		handler: NewHandler(getTenant, NewService(nativeQuerier)),
	}
}

type logPatternsModule struct {
	handler *Handler
}

func (m *logPatternsModule) Name() string                      { return "logPatterns" }
func (m *logPatternsModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logPatternsModule) RegisterRoutes(group *gin.RouterGroup) {
	group.POST("/logs/patterns", m.handler.GetPatterns)
}
