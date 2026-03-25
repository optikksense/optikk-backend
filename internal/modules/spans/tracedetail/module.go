package tracedetail

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *TraceDetailHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/traces/:traceId/span-events", h.GetSpanEvents)
	v1.GET("/traces/:traceId/span-kind-breakdown", h.GetSpanKindBreakdown)
	v1.GET("/traces/:traceId/critical-path", h.GetCriticalPath)
	v1.GET("/traces/:traceId/span-self-times", h.GetSpanSelfTimes)
	v1.GET("/traces/:traceId/error-path", h.GetErrorPath)
	v1.GET("/traces/:traceId/spans/:spanId/attributes", h.GetSpanAttributes)
	v1.GET("/traces/:traceId/flamegraph", h.GetFlamegraphData)
	v1.GET("/traces/:traceId/related", h.GetRelatedTraces)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &traceDetailModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type traceDetailModule struct {
	handler *TraceDetailHandler
}

func (m *traceDetailModule) Name() string                      { return "traceDetail" }
func (m *traceDetailModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *traceDetailModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &TraceDetailHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *traceDetailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
