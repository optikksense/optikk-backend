package tracecompare

import (
	"github.com/gin-gonic/gin"
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

	v1.GET("/traces/compare", h.GetTraceComparison)
}

func init() {
	registry.Register(&traceCompareModule{})
}

type traceCompareModule struct {
	handler *Handler
}

func (m *traceCompareModule) Name() string                      { return "traceCompare" }
func (m *traceCompareModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *traceCompareModule) Init(deps registry.Deps) error {
	m.handler = NewHandler(
		deps.GetTenant,
		NewService(NewRepository(deps.NativeQuerier)),
	)
	return nil
}

func (m *traceCompareModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
