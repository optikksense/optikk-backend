package explorer

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

// NewModule creates the metrics explorer module.
func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	return &metricsExplorerModule{
		handler: NewHandler(getTenant, NewService(nativeQuerier)),
	}
}

type metricsExplorerModule struct {
	handler *Handler
}

func (m *metricsExplorerModule) Name() string                      { return "metricsExplorer" }
func (m *metricsExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *metricsExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	group.GET("/metrics/names", m.handler.GetMetricNames)
	group.GET("/metrics/:metricName/tags", m.handler.GetMetricTags)
	group.POST("/metrics/explorer/query", m.handler.Query)
}
