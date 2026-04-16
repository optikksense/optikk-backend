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
	v1.POST("/explorer/logs/analytics", h.Analytics)
	v1.GET("/logs/volume", h.GetLogVolume)
	v1.GET("/logs/stats", h.GetLogStats)
	v1.GET("/logs/aggregate", h.GetLogAggregate)
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	searchService := logsearch.NewService(logsearch.NewRepository(deps.NativeQuerier))
	logStatsService := newLogStatsService(deps.NativeQuerier)
	return &logsExplorerModule{
		handler: NewHandler(deps.GetTenant, NewService(searchService, logStatsService), logStatsService, deps.NativeQuerier),
	}, nil
}

type logsExplorerModule struct {
	handler *Handler
}

func (m *logsExplorerModule) Name() string                      { return "logsExplorer" }
func (m *logsExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logsExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
