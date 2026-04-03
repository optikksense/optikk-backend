package metricsexplorer

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &metricsExplorerModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type metricsExplorerModule struct {
	handler *Handler
}

func (m *metricsExplorerModule) Name() string                      { return "metricsExplorer" }
func (m *metricsExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *metricsExplorerModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		GetTenant: getTenant,
		Service:   NewService(NewRepository(nativeQuerier)),
	}
}

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
	g := v1.Group("/metrics-explorer")
	g.GET("/metric-names", h.ListMetricNames)
	g.GET("/tag-keys", h.ListTagKeys)
	g.GET("/tag-values", h.ListTagValues)
	g.POST("/query", h.Query)
}

func (m *metricsExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
