package deployments

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
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

func (m *deploymentsModule) Name() string                      { return "deployments" }
func (m *deploymentsModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *deploymentsModule) RegisterRoutes(group *gin.RouterGroup) {
	d := group.Group("/deployments")
	d.GET("/list", m.handler.ListDeployments)
	d.GET("/timeline", m.handler.GetVersionTraffic)
	d.GET("/impact", m.handler.GetDeploymentImpact)
	d.GET("/active-version", m.handler.GetActiveVersion)
}
