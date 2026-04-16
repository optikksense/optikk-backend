package explorer

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	spantraces "github.com/Optikk-Org/optikk-backend/internal/modules/traces/query"
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
	v1.POST("/traces/explorer/query", h.Query)
	v1.POST("/explorer/traces/analytics", h.Analytics)
}

func NewModule(deps *registry.Deps) (registry.Module, error) {
	module := &tracesExplorerModule{}
	module.configure(deps)
	return module, nil
}

type tracesExplorerModule struct {
	handler *Handler
}

func (m *tracesExplorerModule) Name() string                      { return "tracesExplorer" }
func (m *tracesExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *tracesExplorerModule) configure(deps *registry.Deps) {
	traceService := spantraces.NewService(spantraces.NewRepository(deps.NativeQuerier))
	m.handler = NewHandler(deps.GetTenant, NewService(traceService), deps.NativeQuerier)
}

func (m *tracesExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
