package explorer

import (
	"github.com/gin-gonic/gin"
	spanlivetail "github.com/observability/observability-backend-go/internal/modules/spans/livetail"
	spantraces "github.com/observability/observability-backend-go/internal/modules/spans/traces"
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
	v1.POST("/traces/explorer/query", h.Query)
	v1.GET("/traces/explorer/stream", h.Stream)
}

func init() {
	registry.Register(&tracesExplorerModule{})
}

type tracesExplorerModule struct {
	handler *Handler
}

func (m *tracesExplorerModule) Name() string                      { return "tracesExplorer" }
func (m *tracesExplorerModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *tracesExplorerModule) Init(deps registry.Deps) error {
	traceService := spantraces.NewService(spantraces.NewRepository(deps.NativeQuerier))
	liveTailService := spanlivetail.NewService(spanlivetail.NewRepository(deps.NativeQuerier))
	m.handler = NewHandler(
		deps.GetTenant,
		NewService(traceService, liveTailService),
		liveTailService,
	)
	return nil
}

func (m *tracesExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
