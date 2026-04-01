package connections

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}
	shared.RegisterDualGroup(v1, "/connections", func(g *gin.RouterGroup) {
		g.GET("/count", h.GetConnectionCountSeries)
		g.GET("/utilization", h.GetConnectionUtilization)
		g.GET("/limits", h.GetConnectionLimits)
		g.GET("/pending", h.GetPendingRequests)
		g.GET("/timeout-rate", h.GetConnectionTimeoutRate)
		g.GET("/wait-time", h.GetConnectionWaitTime)
		g.GET("/create-time", h.GetConnectionCreateTime)
		g.GET("/use-time", h.GetConnectionUseTime)
	})
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &dbConnectionsModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type dbConnectionsModule struct {
	handler *Handler
}

func (m *dbConnectionsModule) Name() string                      { return "dbConnections" }
func (m *dbConnectionsModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbConnectionsModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *dbConnectionsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
