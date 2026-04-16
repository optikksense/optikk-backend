package metrics

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

func NewModule(deps *registry.Deps) (registry.Module, error) {
	return &metricsExplorerModule{
		handler: &Handler{
			GetTenant: deps.GetTenant,
			Service:   NewService(NewRepository(deps.NativeQuerier)),
		},
	}, nil
}

type metricsExplorerModule struct {
	handler *Handler
}

func (m *metricsExplorerModule) Name() string                      { return "metricsExplorer" }
func (m *metricsExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *metricsExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	g := group.Group("/metrics")
	g.GET("/names", m.handler.ListMetricNames)
	g.GET("/:metricName/tags", m.handler.ListTags)
	g.POST("/explorer/query", m.handler.Query)
}
