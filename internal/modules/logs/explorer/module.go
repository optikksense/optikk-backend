package explorer

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	logsearch "github.com/Optikk-Org/optikk-backend/internal/modules/logs/search"
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
	v1.POST("/logs/explorer/query", h.Query)
	v1.GET("/logs/volume", h.GetLogVolume)
	v1.GET("/logs/stats", h.GetLogStats)
	v1.GET("/logs/aggregate", h.GetLogAggregate)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &logsExplorerModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type logsExplorerModule struct {
	handler *Handler
}

func (m *logsExplorerModule) Name() string                      { return "logsExplorer" }
func (m *logsExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logsExplorerModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	searchService := logsearch.NewService(logsearch.NewRepository(nativeQuerier))
	logStatsService := newLogStatsService(nativeQuerier)
	m.handler = NewHandler(getTenant, NewService(searchService, logStatsService), logStatsService)
}

func (m *logsExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
