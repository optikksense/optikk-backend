package system

import (
	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/registry"
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
	g := v1.Group("/database/system")
	g.GET("/latency", h.GetSystemLatency)
	g.GET("/ops", h.GetSystemOps)
	g.GET("/top-collections-by-latency", h.GetSystemTopCollectionsByLatency)
	g.GET("/top-collections-by-volume", h.GetSystemTopCollectionsByVolume)
	g.GET("/errors", h.GetSystemErrors)
	g.GET("/namespaces", h.GetSystemNamespaces)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &dbSystemModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type dbSystemModule struct {
	handler *Handler
}

func (m *dbSystemModule) Name() string                      { return "dbSystem" }
func (m *dbSystemModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *dbSystemModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *dbSystemModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
