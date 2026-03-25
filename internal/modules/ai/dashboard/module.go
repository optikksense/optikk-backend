package dashboard

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

	v1.GET("/ai/summary", h.GetAISummary)
	v1.GET("/ai/models", h.GetAIModels)
	v1.GET("/ai/performance/metrics", h.GetAIPerformanceMetrics)
	v1.GET("/ai/performance/timeseries", h.GetAIPerformanceTimeSeries)
	v1.GET("/ai/performance/latency-histogram", h.GetAILatencyHistogram)
	v1.GET("/ai/cost/metrics", h.GetAICostMetrics)
	v1.GET("/ai/cost/timeseries", h.GetAICostTimeSeries)
	v1.GET("/ai/cost/token-breakdown", h.GetAITokenBreakdown)
	v1.GET("/ai/security/metrics", h.GetAISecurityMetrics)
	v1.GET("/ai/security/timeseries", h.GetAISecurityTimeSeries)
	v1.GET("/ai/security/pii-categories", h.GetAIPiiCategories)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &aiDashboardModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type aiDashboardModule struct {
	handler *Handler
}

func (m *aiDashboardModule) Name() string                      { return "aiDashboard" }
func (m *aiDashboardModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *aiDashboardModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *aiDashboardModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
