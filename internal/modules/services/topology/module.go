package topology

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	m := &topologyModule{}
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
	return m
}

type topologyModule struct {
	handler *Handler
}

func (m *topologyModule) Name() string                      { return "services_topology" }
func (m *topologyModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *topologyModule) RegisterRoutes(group *gin.RouterGroup) {
	g := group.Group("/services/topology")
	g.GET("", m.handler.GetTopology)
}
