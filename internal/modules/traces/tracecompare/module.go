package tracecompare

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
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

	v1.GET("/traces/compare", h.GetTraceComparison)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &traceCompareModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type traceCompareModule struct {
	handler *Handler
}

func (m *traceCompareModule) Name() string                      { return "traceCompare" }
func (m *traceCompareModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *traceCompareModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = NewHandler(
		getTenant,
		NewService(NewRepository(nativeQuerier)),
	)
}

func (m *traceCompareModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
