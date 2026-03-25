package traces

import (
	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
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

	v1.GET("/ai/traces/:traceId", h.GetLLMTrace)
	v1.GET("/ai/traces/:traceId/summary", h.GetLLMTraceSummary)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &aiTracesModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type aiTracesModule struct {
	handler *Handler
}

func (m *aiTracesModule) Name() string                      { return "aiTraces" }
func (m *aiTracesModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *aiTracesModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *aiTracesModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
