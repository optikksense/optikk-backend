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

func init() {
	registry.Register(&logsExplorerModule{})
}

type logsExplorerModule struct {
	handler *Handler
}

func (m *logsExplorerModule) Name() string                      { return "logsExplorer" }
func (m *logsExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logsExplorerModule) Init(deps registry.Deps) error {
	searchService := logsearch.NewService(logsearch.NewRepository(deps.NativeQuerier))
	analyticsService := loganalytics.NewService(loganalytics.NewRepository(deps.NativeQuerier))
	m.handler = NewHandler(
		deps.GetTenant,
		NewService(searchService, analyticsService),
		searchService,
	)
	return nil
}

func (m *logsExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
