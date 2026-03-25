package explorer

import (
	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/registry"
	spanlivetail "github.com/observability/observability-backend-go/internal/modules/spans/livetail"
	spantraces "github.com/observability/observability-backend-go/internal/modules/spans/traces"
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
	v1.GET("/traces/explorer/stream", h.Stream)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &tracesExplorerModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type tracesExplorerModule struct {
	handler *Handler
}

func (m *tracesExplorerModule) Name() string                      { return "tracesExplorer" }
func (m *tracesExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *tracesExplorerModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	traceService := spantraces.NewService(spantraces.NewRepository(nativeQuerier))
	liveTailService := spanlivetail.NewService(spanlivetail.NewRepository(nativeQuerier))
	m.handler = NewHandler(
		getTenant,
		NewService(traceService, liveTailService),
		liveTailService,
	)
}

func (m *tracesExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
