package explorer

import (
	"github.com/gin-gonic/gin"
	loganalytics "github.com/observability/observability-backend-go/internal/modules/log/analytics"
	logsearch "github.com/observability/observability-backend-go/internal/modules/log/search"
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
	v1.POST("/logs/explorer/query", h.Query)
	v1.GET("/logs/explorer/stream", h.Stream)
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
	analyticsService := loganalytics.NewService(loganalytics.NewRepository(nativeQuerier))
	m.handler = NewHandler(
		getTenant,
		NewService(searchService, analyticsService),
		searchService,
	)
}

func (m *logsExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
