package deployments

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	m := &deploymentsModule{}
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
	return m
}

type deploymentsModule struct {
	handler *Handler
}

func (m *deploymentsModule) Name() string { return "deployments" }

func (m *deploymentsModule) RegisterRoutes(group *gin.RouterGroup) {
	d := group.Group("/deployments")
	d.GET("/latest-by-service", m.handler.ListLatestDeploymentsByService)
	d.GET("/list", m.handler.ListDeployments)
	d.GET("/compare", m.handler.GetDeploymentCompare)
	d.GET("/timeline", m.handler.GetVersionTraffic)
	d.GET("/impact", m.handler.GetDeploymentImpact)
	d.GET("/active-version", m.handler.GetActiveVersion)
}
