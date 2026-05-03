package redmetrics

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
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
	red.GET("/apdex", h.GetApdex)
	red.GET("/top-slow-operations", h.GetTopSlowOperations)
	red.GET("/top-error-operations", h.GetTopErrorOperations)
	red.GET("/request-rate", h.GetRequestRateTimeSeries)
	red.GET("/p95-latency", h.GetP95LatencyTimeSeries)
	red.GET("/span-kind-breakdown", h.GetSpanKindBreakdown)
	red.GET("/errors-by-route", h.GetErrorsByRoute)

	v1.GET("/spans/latency-breakdown", h.GetLatencyBreakdown)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &redMetricsModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type redMetricsModule struct {
	handler *REDMetricsHandler
}

func (m *redMetricsModule) Name() string { return "redMetrics" }

func (m *redMetricsModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &REDMetricsHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewService(NewRepository(nativeQuerier)),
	}
}

func (m *redMetricsModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
