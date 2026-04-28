package explorer

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &metricsExplorerModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type metricsExplorerModule struct {
	handler *Handler
}

func (m *metricsExplorerModule) Name() string                      { return "metricsExplorer" }

func (m *metricsExplorerModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		GetTenant: getTenant,
		Service:   NewService(NewRepository(nativeQuerier)),
	}
}

func (m *metricsExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	g := group.Group("/metrics")
	g.GET("/names", m.handler.ListMetricNames)
	g.GET("/:metricName/tags", m.handler.ListTags)
	g.POST("/explorer/query", m.handler.Query)
}
