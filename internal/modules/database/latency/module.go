package latency

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
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
	g := v1.Group("/database/latency")
	g.GET("/by-system", h.GetLatencyBySystem)
	g.GET("/by-operation", h.GetLatencyByOperation)
	g.GET("/by-collection", h.GetLatencyByCollection)
	g.GET("/by-namespace", h.GetLatencyByNamespace)
	g.GET("/by-server", h.GetLatencyByServer)
	g.GET("/heatmap", h.GetLatencyHeatmap)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &dbLatencyModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type dbLatencyModule struct {
	handler *Handler
}

func (m *dbLatencyModule) Name() string                      { return "dbLatency" }
func (m *dbLatencyModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbLatencyModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *dbLatencyModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
