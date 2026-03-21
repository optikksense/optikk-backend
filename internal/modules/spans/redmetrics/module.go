package redmetrics

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

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *REDMetricsHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	red := v1.Group("/spans/red")
	red.GET("/summary", h.GetSummary)
	red.GET("/service-scorecard", h.GetServiceScorecard)
	red.GET("/apdex", h.GetApdex)
	red.GET("/http-status-distribution", h.GetHTTPStatusDistribution)
	red.GET("/top-slow-operations", h.GetTopSlowOperations)
	red.GET("/top-error-operations", h.GetTopErrorOperations)
	red.GET("/request-rate", h.GetRequestRateTimeSeries)
	red.GET("/error-rate", h.GetErrorRateTimeSeries)
	red.GET("/p95-latency", h.GetP95LatencyTimeSeries)
	red.GET("/span-kind-breakdown", h.GetSpanKindBreakdown)
	red.GET("/errors-by-route", h.GetErrorsByRoute)

	v1.GET("/spans/latency-breakdown", h.GetLatencyBreakdown)
}

func init() {
	registry.Register(&redMetricsModule{})
}

type redMetricsModule struct {
	handler *REDMetricsHandler
}

func (m *redMetricsModule) Name() string                      { return "redMetrics" }
func (m *redMetricsModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *redMetricsModule) Init(deps registry.Deps) error {
	m.handler = &REDMetricsHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: deps.GetTenant},
		Service:  NewService(NewRepository(deps.NativeQuerier)),
	}
	return nil
}

func (m *redMetricsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
